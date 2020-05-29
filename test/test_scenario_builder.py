from utils.ScenarioBuilder import ScenarioBuilder


def test_scenario_builder_does_populate_database():
    builder = ScenarioBuilder()

    builder.reset()
    builder.insert_random_nhs_records(count=10)
    builder.insert_random_ivr_records(count=10)
    builder.insert_random_web_records(count=10)
    builder.insert_random_la_feedback(count=10)
