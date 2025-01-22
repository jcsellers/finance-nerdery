from strategy_orchestrator import StrategyOrchestrator

orchestrator = StrategyOrchestrator(
    config_path="config/test.json", data_path="data/test.csv"
)
print("Orchestrator initialized successfully!")
