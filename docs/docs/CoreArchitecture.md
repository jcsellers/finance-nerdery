
# Design Document: Modular Backtesting and Simulation Framework

## Purpose
To build a modular, scalable framework that integrates custom trading algorithms with robust simulation and performance analysis capabilities. This system leverages the best features of libraries like Zipline, Pyfolio, and TA-Lib while maintaining compatibility with the project's existing data pipeline and supporting future expansion needs.

---

## High-Level Architecture

### 1. Data Ingestion Layer
- **Source**: Local database (CSV files), FRED API, Quandl API.
- **Functionality**:
  - Load historical data in a Zipline-compatible format.
  - Create custom data bundles for ingesting external sources (FRED).
- **Tools**: Pandas, custom adapters for database integration.

### 2. Core Simulation Engine
- **Components**:
  - **Event Loop**: Zipline's event-driven framework manages time steps, data feeding, and portfolio updates.
  - **Rebalancing**: Scheduled rebalancing functions using `schedule_function()` API.
- **Responsibilities**:
  - Execute algorithms in a sandboxed environment.
  - Provide portfolio tracking, trade execution, and logging.
- **Integration**: Use Zipline-Reloaded for active maintenance.

### 3. Modular Algorithm Interface
- **Structure**: Single-file algorithms adhering to predefined entry points (`initialize()`, `handle_data()`).
- **Features**:
  - Support for custom trading rules and strategies.
  - Access to preprocessed data, portfolio, and execution APIs.
- **Customization**:
  - Extend Zipline’s default interface with additional hooks (`on_trade()`, `on_rebalance()`).

### 4. Performance and Metrics Module
- **Standard Metrics**:
  - Leverage Pyfolio for portfolio analysis (Sharpe, alpha, beta, drawdowns).
  - Use TA-Lib for technical indicators.
- **Custom Metrics**:
  - Modular `MetricsModule` allows for plug-and-play custom analytics.
- **Outputs**:
  - Tear sheets, visualizations, and CSV reports.

---

## Key Features

### 1. Custom Algorithm Integration
- Algorithms are self-contained Python scripts.
- Define trading logic via `initialize()` and `handle_data()` or custom hooks.
- Example:
```python
    def initialize(context):
        context.asset = symbol('AAPL')
        context.target_percent = 0.5

    def handle_data(context, data):
        if not context.portfolio.positions:
            order_target_percent(context.asset, context.target_percent)
```

### 2. Data Compatibility
- Local database remains the primary data source.
- FRED and Quandl APIs support additional data ingestion.
- Custom adapters ensure seamless integration.

### 3. Simulation Granularity
- Daily or minute-level simulations supported.
- Scheduling capabilities for rebalancing.

### 4. Extensibility
- Easily add new metrics or trading strategies.
- Hooks for future features like state persistence.

---

## Workflow

### 1. Data Preparation
- CSV files from the local database are loaded via custom Zipline bundles.
- Optional: Use FRED/Quandl APIs for supplementary data.

### 2. Simulation Execution
- User selects a custom algorithm and simulation parameters.
- Zipline runs the simulation step-by-step:
  - Feeds data to the algorithm.
  - Executes trades based on logic.
  - Tracks portfolio performance.

### 3. Metrics and Reporting
- Pyfolio analyzes portfolio performance.
- Custom metrics are calculated via the `MetricsModule`.
- Outputs include visualizations, CSV reports, and tear sheets.

---

## Dependencies

### Repository Information
- **Repository**: The project repository is located at: https://github.com/jcsellers/finance-nerdery
- **Database**: The existing database file can be found at `data/aligned/trading_database.db`.

### Core Libraries
- **Zipline-Reloaded**: Backtesting and simulation engine.
- **Pyfolio**: Performance and risk analysis.
- **TA-Lib**: Technical indicators.

### Data Handling
- Pandas, NumPy, SciPy: Data manipulation and computation.

---

## Deliverables for Developer

### Code Templates
1. Custom data adapter for CSV and FRED integration.
   - Include unit tests to confirm data ingestion correctness and compatibility with Zipline.
   - Ensure the adapter works seamlessly with the database located at `data/aligned/trading_database.db`.
   - Include unit tests to confirm data ingestion correctness and compatibility with Zipline.
2. Example algorithm file.

### Documentation
1. High-level architecture diagram.
2. Instructions for creating Zipline-compatible data bundles.
3. API reference for Modular Algorithm Interface.

### Test Cases
1. Example: Buy-and-Hold strategy with daily and monthly rebalancing.
   - Ensure unit tests validate functionality and edge cases of the strategy.
   - Verify compatibility with the existing CI/CD pipeline.

---

## Diagrams

### High-Level Architecture Flow

#### Components:
1. **Data Ingestion Layer**: Local database -> CSV files -> Zipline Data Bundles.
2. **Core Simulation Engine**: Event loop -> Algorithms -> Portfolio.
3. **Metrics Module**: Pyfolio -> TA-Lib -> Custom Metrics.
4. **Outputs**: Reports -> Visualizations -> Logs.

#### Example Flow:
- Historical data is loaded from CSV or APIs.
- Data flows into Zipline for simulation.
- Algorithms execute logic step-by-step in an event-driven loop.
- Metrics are calculated and logged for reporting.

---

## Next Steps
- [ ] Create initial data adapter for CSV integration.
   - Update CI/CD pipeline configuration to run all new unit tests automatically during builds.
- [ ] Develop a basic Buy-and-Hold strategy as a test algorithm.
- [ ] Set up Pyfolio integration for performance analysis.
