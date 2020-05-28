from utils.generators import generate_nhs_clean_entry, generate_web_clean_entry


def test_generate_nhs_clean_entry():
    entry = generate_nhs_clean_entry()

    assert entry is not None


def test_generate_web_clean_entry():
    entry = generate_web_clean_entry()

    assert entry is not None
