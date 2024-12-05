package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"syscall"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)
type Store struct {
    ID        int64            `json:"id"`
    Name      string           `json:"name"`
    Contact   pgtype.Text      `json:"contact"`
    OpenAt    pgtype.Timestamp `json:"open_at"`
    CloseAt   pgtype.Timestamp `json:"close_at"`
    Status    string           `json:"status"`
}

type CreateStoreRequest struct {
    Name     string           `json:"name"`
    Contact  pgtype.Text      `json:"contact"`
    OpenAt   pgtype.Timestamp `json:"open_at"`
    CloseAt  pgtype.Timestamp `json:"close_at"`
}

type Logger struct {
	*zap.Logger
	sugar *zap.SugaredLogger
}

type StoreRepository struct {
    db     *DatabaseClient
    logger *Logger
	getStoreByIDStmt string
	getStoresStmt    string
}

type DatabaseClient struct {
    Pool *pgxpool.Pool
}

// ProcessTracker manages and tracks running processes
type ProcessTracker struct {
	mu             sync.RWMutex
	activeContexts map[string]context.CancelFunc
	logger         *Logger
}

// ProcessMetadata provides additional information about a tracked process
type ProcessMetadata struct {
	ID        string
	StartTime time.Time
	Timeout   time.Duration
}

type Config struct {
	// Database Configuration
	DBHost     string
	DBPort     int
	DBUser     string
	DBPassword string
	DBName     string
	DBSSLMode  string

	// Connection Pool Settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnTimeout     time.Duration

	// Server Configuration
	ServerPort int
	Environment string
}

// NewProcessTracker creates a new ProcessTracker
func NewProcessTracker(log *Logger) *ProcessTracker {
	return &ProcessTracker{
		activeContexts: make(map[string]context.CancelFunc),
		logger:         log,
	}
}

// AddContext registers a new context with a unique identifier
func (pt *ProcessTracker) AddContext(id string, cancel context.CancelFunc) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	
	// Check if the ID already exists
	if _, exists := pt.activeContexts[id]; exists {
		pt.logger.Warn("Context with ID already exists", 
			zap.String("processID", id))
		return fmt.Errorf("context with ID %s already exists", id)
	}
	
	pt.activeContexts[id] = cancel
	pt.logger.Debug("Context added", 
		zap.String("processID", id))
	return nil
}

// RemoveContext removes a context from tracking
func (pt *ProcessTracker) RemoveContext(id string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	
	pt.logger.Debug("Removing context", 
		zap.String("processID", id))
	delete(pt.activeContexts, id)
}

// ListActiveProcesses returns a list of active process IDs
func (pt *ProcessTracker) ListActiveProcesses() []string {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	
	processes := make([]string, 0, len(pt.activeContexts))
	for id := range pt.activeContexts {
		processes = append(processes, id)
	}
	return processes
}

// CancelAllContexts cancels all tracked contexts
func (pt *ProcessTracker) CancelAllContexts() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	for id, cancel := range pt.activeContexts {
		pt.logger.Info("Cancelling process", 
			zap.String("processID", id))
		cancel()
		delete(pt.activeContexts, id)
	}
}

// EnforceShutdown provides a controlled shutdown mechanism
func (pt *ProcessTracker) EnforceShutdown(force bool) error {
	activeProcesses := pt.ListActiveProcesses()
	
	if len(activeProcesses) > 0 {
		pt.logger.Warn("Active processes detected", 
			zap.Strings("processes", activeProcesses))
		
		if !force {
			return fmt.Errorf("cannot shutdown. Active processes: %v", activeProcesses)
		}
	}

	pt.CancelAllContexts()
	return nil
}

// WithTimeoutTracking extends ProcessTracker to track process timeouts
type WithTimeoutTracking struct {
	*ProcessTracker
	metadata     map[string]ProcessMetadata
	cleanupChan  chan string
	stopCleanup  chan struct{}
}

// NewWithTimeoutTracking creates a process tracker with timeout tracking
func NewWithTimeoutTracking(log *Logger, cleanupInterval time.Duration) *WithTimeoutTracking {
	pt := &WithTimeoutTracking{
		ProcessTracker: NewProcessTracker(log),
		metadata:       make(map[string]ProcessMetadata),
		cleanupChan:    make(chan string, 100),
		stopCleanup:    make(chan struct{}),
	}

	go pt.startTimeoutCleanup(cleanupInterval)
	return pt
}

// AddContextWithTimeout adds a context with timeout tracking
func (pt *WithTimeoutTracking) AddContextWithTimeout(id string, cancel context.CancelFunc, timeout time.Duration) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if _, exists := pt.activeContexts[id]; exists {
		return fmt.Errorf("context with ID %s already exists", id)
	}

	pt.activeContexts[id] = cancel
	pt.metadata[id] = ProcessMetadata{
		ID:        id,
		StartTime: time.Now(),
		Timeout:   timeout,
	}

	// Schedule potential timeout cleanup
	go func() {
		select {
		case <-time.After(timeout):
			pt.cleanupChan <- id
		case <-pt.stopCleanup:
			return
		}
	}()

	return nil
}

// startTimeoutCleanup handles periodic cleanup of expired processes
func (pt *WithTimeoutTracking) startTimeoutCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-pt.stopCleanup:
			return
		case id := <-pt.cleanupChan:
			pt.mu.Lock()
			if metadata, exists := pt.metadata[id]; exists {
				if time.Since(metadata.StartTime) >= metadata.Timeout {
					pt.logger.Warn("Process timeout detected", 
						zap.String("processID", id),
						zap.Duration("duration", time.Since(metadata.StartTime)))
					
					// Cancel the context if it's still active
					if cancel, exists := pt.activeContexts[id]; exists {
						cancel()
						delete(pt.activeContexts, id)
						delete(pt.metadata, id)
					}
				}
			}
			pt.mu.Unlock()
		case <-ticker.C:
			// Periodic cleanup of long-running processes
			pt.cleanExpiredProcesses()
		}
	}
}

// cleanExpiredProcesses removes processes that have exceeded their timeout
func (pt *WithTimeoutTracking) cleanExpiredProcesses() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	now := time.Now()
	for id, metadata := range pt.metadata {
		if now.Sub(metadata.StartTime) > metadata.Timeout {
			pt.logger.Warn("Cleaning up expired process", 
				zap.String("processID", id),
				zap.Duration("runTime", now.Sub(metadata.StartTime)))
			
			// Cancel the context
			if cancel, exists := pt.activeContexts[id]; exists {
				cancel()
				delete(pt.activeContexts, id)
				delete(pt.metadata, id)
			}
		}
	}
}

// Stop stops the timeout tracking and cleanup goroutines
func (pt *WithTimeoutTracking) Stop() {
	close(pt.stopCleanup)
}


func NewLogger(environment string) *Logger {
	var config zap.Config

	if environment == "production" {
		config = zap.NewProductionConfig()
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	logger, err := config.Build()
	if err != nil {
		panic(err)
	}

	return &Logger{
		Logger: logger,
		sugar:  logger.Sugar(),
	}
}

func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.Logger.Info(msg, fields...)
}

func (l *Logger) Error(msg string, fields ...zap.Field) {
	l.Logger.Error(msg, fields...)
}

func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.Logger.Warn(msg, fields...)
}

func (l *Logger) Debug(msg string, fields ...zap.Field) {
	l.Logger.Debug(msg, fields...)
}


func (l *Logger) SugaredLogger() *zap.SugaredLogger {
	return l.sugar
}

func LoadConfig() *Config {
	return &Config{
		// Database Configuration
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnvAsInt("DB_PORT", 5432),
		DBUser:     getEnv("DB_USER", "user"),
		DBPassword: getEnv("DB_PASSWORD", "password"),
		DBName:     getEnv("DB_NAME", "db"),
		DBSSLMode:  getEnv("DB_SSLMODE", "disable"),

		// Connection Pool Settings
		MaxOpenConns:    getEnvAsInt("DB_MAX_OPEN_CONNS", 25),
		MaxIdleConns:    getEnvAsInt("DB_MAX_IDLE_CONNS", 25),
		ConnMaxLifetime: getEnvAsDuration("DB_CONN_MAX_LIFETIME", 15*time.Minute),
		ConnTimeout:     getEnvAsDuration("DB_CONN_TIMEOUT", 5*time.Second),

		// Server Configuration
		ServerPort:   getEnvAsInt("SERVER_PORT", 3000),
		Environment: getEnv("ENV", "development"),
	}
}

// Helper functions to retrieve environment variables
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := getEnv(key, "")
	if value, err := time.ParseDuration(valueStr); err == nil {
		return value
	}
	return defaultValue
}

func NewDatabaseClient(cfg *Config, log *Logger) (*DatabaseClient, error) {
    connStr := fmt.Sprintf(
        "postgres://%s:%s@%s:%d/%s?sslmode=%s&connect_timeout=%d",
        cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, 
        cfg.DBName, cfg.DBSSLMode, int(cfg.ConnTimeout.Seconds()),
    )

    poolConfig, err := pgxpool.ParseConfig(connStr)
    if err != nil {
        return nil, fmt.Errorf("parse connection config: %w", err)
    }

    // Configure connection pool
    poolConfig.MaxConns = int32(cfg.MaxOpenConns)
    poolConfig.MinConns = int32(cfg.MaxIdleConns)
    poolConfig.MaxConnLifetime = cfg.ConnMaxLifetime

    pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
    if err != nil {
        return nil, fmt.Errorf("create connection pool: %w", err)
    }

    // Verify database connection
    if err := pool.Ping(context.Background()); err != nil {
        return nil, fmt.Errorf("ping database: %w", err)
    }

    log.Info("Database connection established successfully")
    return &DatabaseClient{Pool: pool}, nil
}

// PrepareStatement is replaced by direct query methods in pgx
func (dc *DatabaseClient) Exec(ctx context.Context, query string, args ...interface{}) error {
    _, err := dc.Pool.Exec(ctx, query, args...)
    return err
}

func (dc *DatabaseClient) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
    return dc.Pool.Query(ctx, query, args...)
}

func (dc *DatabaseClient) QueryRow(ctx context.Context, query string, args ...interface{}) pgx.Row {
    return dc.Pool.QueryRow(ctx, query, args...)
}

func (dc *DatabaseClient) HealthCheck(ctx context.Context) error {
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    return dc.Pool.Ping(ctx)
}

func (dc *DatabaseClient) Close() error {
    dc.Pool.Close()
    return nil
}

func NewStoreRepository(db *DatabaseClient, logger *Logger) (*StoreRepository, error) {
	getStoreByIDStmt := `
	SELECT 
		id, 
		name, 
		contact, 
		open_at, 
		close_at,
		CASE 
                WHEN NOW() BETWEEN open_at AND close_at THEN 'open'
                ELSE 'closed'
            END AS status 
	FROM store
	WHERE id = @store_id
`

    getStoresStmt := `
        SELECT 
            id, 
            name, 
            contact, 
            open_at, 
            close_at,
            CASE 
                WHEN NOW() BETWEEN open_at AND close_at THEN 'open'
                ELSE 'closed'
            END AS status
        FROM store
        ORDER BY name
        LIMIT $1 OFFSET $2
    `
    
    return &StoreRepository{
        db:     db,
        logger: logger,
		getStoreByIDStmt: getStoreByIDStmt,
        getStoresStmt: getStoresStmt,
    }, nil
}




func (r *StoreRepository) GetStores(ctx context.Context, limit, offset int) ([]Store, error) {
	args := pgx.NamedArgs{
		"limit":  limit,
		"offset": offset,
	}

	rows, err := r.db.Query(ctx, r.getStoresStmt, args)
	if err != nil {
		r.logger.Error("Failed to get stores",
			zap.Int("limit", limit),
			zap.Int("offset", offset),
			zap.Error(err),
		)
		return nil, err
	}
	defer rows.Close()

	stores := make([]Store, 0, limit)
	// var stores []Store
	for rows.Next() {
		var store Store
		if err := rows.Scan(
			&store.ID, 
			&store.Name, 
			&store.Contact, 
			&store.OpenAt, 
			&store.CloseAt,
			&store.Status,
		); err != nil {
			r.logger.Error("Failed to scan store data", zap.Error(err))
			return nil, err
		}
		
		
		
		stores = append(stores, store)
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("Error during rows iteration", zap.Error(err))
		return nil, err
	}

	return stores, nil
}

// GetStoreByID retrieves a store record by its ID
func (r *StoreRepository) GetStoreByID(ctx context.Context, storeID int64) (*Store, error) {
	args := pgx.NamedArgs{
		"store_id": storeID,
	}

	var store Store
	err := r.db.QueryRow(ctx, r.getStoreByIDStmt, args).Scan(
		&store.ID,
		&store.Name,
		&store.Contact,
		&store.OpenAt,
		&store.CloseAt,
		&store.Status,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		r.logger.Error("Failed to get store by ID",
			zap.Int64("storeID", storeID),
			zap.Error(err),
		)
		return nil, err
	}

	

	return &store, nil
}




func main() {
	// Load configuration
	cfg := LoadConfig()

	// Initialize logger
	appLogger := NewLogger(cfg.Environment)
	defer appLogger.Logger.Sync()

	// Initialize process tracker with timeout tracking
    cleanupInterval := 1 * time.Minute
    processTracker := NewWithTimeoutTracking(appLogger, cleanupInterval)


    // Ensure cleanup goroutines are stopped on exit
    defer processTracker.Stop()

	// Initialize database client
	dbClient, err := NewDatabaseClient(cfg, appLogger)
	if err != nil {
		appLogger.Error("Failed to initialize database",
			zap.Error(err),
		)
		os.Exit(1)
	}

	// Initialize repositories
	storeRepo, err := NewStoreRepository(dbClient, appLogger)
	if err != nil {
		appLogger.Error("Failed to initialize store repository",
			zap.Error(err),
		)
		dbClient.Close() // Ensure database connection is closed on error
		os.Exit(1)
	}

	

	// Create Fiber app
	app := fiber.New(fiber.Config{
		
		AppName:               "176org",
		ServerHeader:          "176org",
		DisableStartupMessage: false,
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          10 * time.Second,
		IdleTimeout:           20 * time.Second,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})


	// Middleware
	app.Use(recover.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
	}))

	// Health check route
	app.Get("/health", func(c *fiber.Ctx) error {
		// Check database health
		if err := dbClient.HealthCheck(c.Context()); err != nil {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"status": "unhealthy",
				"error":  err.Error(),
			})
		}
		return c.JSON(fiber.Map{
			"status":    "healthy",
			"timestamp": time.Now(),
		})
	})

	

	// Route to get a store by ID
	app.Get("/stores/:id", func(c *fiber.Ctx) error {
		// Create a unique context for this request
		ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
		processID := fmt.Sprintf("get-store-%d", time.Now().UnixNano())
		processTracker.AddContext(processID, cancel)
		defer func() {
			cancel()
			processTracker.RemoveContext(processID)
		}()

		storeID, err := c.ParamsInt("id")
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid store ID",
			})
		}

		store, err := storeRepo.GetStoreByID(ctx, int64(storeID))
		if err != nil {
			appLogger.Error("Failed to retrieve store",
				zap.Error(err),
			)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to retrieve store",
			})
		}

		if store == nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Store not found",
			})
		}

		return c.JSON(store)
	})

	// Route to get a list of stores
	app.Get("/stores", func(c *fiber.Ctx) error {
		// Create a unique context for this request
		ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
		processID := fmt.Sprintf("get-stores-%d", time.Now().UnixNano())
		processTracker.AddContext(processID, cancel)
		defer func() {
			cancel()
			processTracker.RemoveContext(processID)
		}()

		// Parse query parameters for pagination
		limit := 20 // Default limit of 20 stores per page
		page := c.QueryInt("page", 1) // Default page is 1

		// Calculate offset
		offset := (page - 1) * limit

		// Fetch stores with pagination
		storeChan := make(chan []Store, 1)
    errChan := make(chan error, 1)

    go func() {
        stores, err := storeRepo.GetStores(ctx, limit, offset)
        if err != nil {
            errChan <- err
            return
        }
        storeChan <- stores
    }()

    // Wait for result with timeout
    select {
    case stores := <-storeChan:
        if len(stores) == 0 {
            return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
                "message": "No stores found",
                "total":   0,
            })
        }

        // Optimized response with pagination metadata
        return c.JSON(fiber.Map{
            "stores": stores,
            "pagination": fiber.Map{
                "page":  page,
                "limit": limit,
                "total": len(stores),
            },
        })

    case err := <-errChan:
        appLogger.Error("Failed to retrieve stores", zap.Error(err))
        return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
            "error": "Failed to retrieve stores",
        })

    case <-ctx.Done():
        return c.Status(fiber.StatusRequestTimeout).JSON(fiber.Map{
            "error": "Request timed out",
        })
    }
})



	
	

	// Graceful shutdown setup
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	defer stop()

	// Start server in a goroutine
	serverErrChan := make(chan error, 1)
	go func() {
		serverAddr := fmt.Sprintf(":%d", cfg.ServerPort)
		appLogger.Info("Starting server",
			zap.String("address", serverAddr),
			zap.String("environment", cfg.Environment),
		)

		if err := app.Listen(serverAddr); err != nil {
			serverErrChan <- err
		}
	}()

	// Wait for shutdown or server error
	select {
	case err := <-serverErrChan:
		appLogger.Error("Server failed",
			zap.Error(err),
		)
	case <-ctx.Done():
		appLogger.Info("Shutdown signal received")
	}

	// Attempt graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Enforce shutdown of all processes
	processTracker.EnforceShutdown(true)

	// Shutdown the app
	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		appLogger.Error("SYSTEM-B  shutdown error",
			zap.Error(err),
		)
	}

	// Close database connection
	if err := dbClient.Close(); err != nil {
		appLogger.Error("Database client shutdown error",
			zap.Error(err),
		)
	} else {
		appLogger.Info("Database connection closed successfully")
	}

	appLogger.Info("Server shutdown complete")
}

