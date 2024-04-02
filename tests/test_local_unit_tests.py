import pytest
from tf.gcp.src.utils.data_police_uk_api import DataPoliceUKAPI
from datetime import datetime


@pytest.fixture
def dpuk():
    obj = DataPoliceUKAPI()
    return obj


def test_get_last_updated(dpuk):
    """Test we get a date back in expected format"""
    latest_date = dpuk.get_last_updated()
    try:
        datetime.strptime(latest_date, "%Y-%m-%d")
    except ValueError:
        assert False
    assert True


def test_validate_message(dpuk):
    invalid_msg = {"monhs": ["2022-12", "2022-10", "2022-07"],
                   "data_sets": ["street", "stop-and-search"]}
    with pytest.raises(KeyError):
        dpuk.validate_message(invalid_msg)


def test_get_month_intervals(dpuk):
    interval_start_month = datetime(2017, 4, 1).date()

    records_end_month = datetime.strptime('2023-01-01', '%Y-%m-%d').date()
    interval_months = dpuk.get_month_intervals(interval_start_month, records_end_month)
    expected = ['2017-04', '2020-04', '2023-01']
    assert expected == interval_months

    records_end_month = datetime.strptime('2020-03-01', '%Y-%m-%d').date()
    interval_months = dpuk.get_month_intervals(interval_start_month, records_end_month)
    expected = ['2017-04', '2020-03']
    assert expected == interval_months


def test_get_records_months(dpuk):
    start_month = datetime.strptime('2011-02-01', '%Y-%m-%d').date()
    end_month = datetime.strptime('2011-07-01', '%Y-%m-%d').date()
    records_months = dpuk.get_records_months(start_month, end_month)
    expected = ['2011-02', '2011-03', '2011-04', '2011-05', '2011-06', '2011-07']
    assert expected == records_months


def test_validate_crime_data_sets(dpuk):
    invalid_crime_data_sets = ["stre1et", "outcomes", "stop-and-search"]
    with pytest.raises(Exception):
        dpuk.validate_crime_data_sets(invalid_crime_data_sets)