# Routes configuration
# Define your application routes here

# Home page
get("/", "home#index");

# Health check endpoint
get("/health", "home#health");

# Documentation routes
get("/docs", "docs#index");
get("/docs/getting-started", "docs#getting_started");
get("/docs/api", "docs#api");
get("/docs/clustering", "docs#clustering");
get("/docs/tcp-protocol", "docs#tcp_protocol");
get("/docs/commands", "docs#commands");
get("/docs/persistence", "docs#persistence");
get("/docs/benchmarks", "docs#benchmarks");

print("Routes loaded!");
