from mkpipe_core.plugins.registry import EXTRACTORS, LOADERS

def run_pipeline(config):
    """
    Executes the ETL pipeline based on the provided configuration.
    """
    # Step 1: Extract
    source_type = config["source"]["type"]
    if source_type not in EXTRACTORS:
        raise ValueError(f"Unsupported extractor type: {source_type}")
    extractor = EXTRACTORS[source_type]()
    data = extractor.extract(config["source"]["config"])

    # Step 2: Transform
    # Placeholder for transformation logic
    transformed_data = data  # No-op for now

    # Step 3: Load
    load_type = config["load"]["type"]
    if load_type not in LOADERS:
        raise ValueError(f"Unsupported loader type: {load_type}")
    loader = LOADERS[load_type]()
    loader.load(transformed_data, config["load"]["config"])
