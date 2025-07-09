import datetime
from decimal import Decimal
import sys
import os
import requests
from unittest.mock import MagicMock, patch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
from data.question import (
    clean_null_emails,
    find_invalid_emails,
    get_first_3_letters_of_names,
    get_email_domains,
    concat_name_and_email,
    cast_total_amount_to_integer,
    find_at_position_in_email,
    fill_null_product_category,
    rank_customers_by_spending,
    running_total_per_customer,
    get_electronics_and_appliances,
    get_orders_with_missing_customers,
)

def test_clean_null_emails():
    # Burada sadece çalışıp hata vermemesi önemli
    assert clean_null_emails() is None

def test_find_invalid_emails():
    result = find_invalid_emails()
    assert isinstance(result, list)
    if result:
        assert '@' not in result[0][2]  # Email kolonunda '@' yoksa

def test_get_first_3_letters_of_names():
    result = get_first_3_letters_of_names()
    assert isinstance(result, list)
    if result:
        full_name, short_name = result[0]
        assert isinstance(short_name, str)
        assert len(short_name) <= 3

def test_get_email_domains():
    result = get_email_domains()
    assert isinstance(result, list)
    if result:
        full_name, domain = result[0]
        assert domain.startswith('.') is False  # Domainler '.' ile başlamamalı

def test_concat_name_and_email():
    result = concat_name_and_email()
    assert isinstance(result, list)
    if result:
        full_info = result[0][0]
        assert ' - ' in full_info  # İsim ve email arasında " - " var mı?

def test_cast_total_amount_to_integer():
    result = cast_total_amount_to_integer()
    assert isinstance(result, list)
    if result:
        order_id, total_amount_int = result[0]
        assert isinstance(total_amount_int, int)  # Cast işlemi gerçekten int mi olmuş?

def test_find_at_position_in_email():
    result = find_at_position_in_email()
    assert isinstance(result, list)
    if result:
        full_name, at_position = result[0]
        assert isinstance(at_position, int)
        assert at_position > 0  # '@' mutlaka bir pozisyonda bulunmuş olmalı

def test_fill_null_product_category():
    result = fill_null_product_category()
    assert isinstance(result, list)
    if result:
        product_name, category = result[0]
        assert category != ''  # Boş olmamalı

def test_rank_customers_by_spending():
    result = rank_customers_by_spending()
    assert isinstance(result, list)
    if result:
        customer_id, total_amount, rank = result[0]
        assert isinstance(rank, int)

def test_running_total_per_customer():
    result = running_total_per_customer()
    assert isinstance(result, list)
    if result:
        order_id, customer_id, total_amount, running_total = result[0]
        assert running_total >= total_amount

def test_get_electronics_and_appliances():
    result = get_electronics_and_appliances()
    assert isinstance(result, list)
    if result:
        product_name, type_ = result[0]
        assert type_ in ['Electronics', 'Appliances']

def test_get_orders_with_missing_customers():
    result = get_orders_with_missing_customers()
    assert isinstance(result, list)
    if result:
        customer_id, total_amount = result[0]
        assert isinstance(total_amount, (int, float, Decimal))

def send_post_request(url: str, data: dict, headers: dict = None):
    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()  # hata varsa exception fırlatır
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status Code: {response.status_code}")
    except Exception as err:
        print(f"Other error occurred: {err}")


class ResultCollector:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    def pytest_runtest_logreport(self, report):
        if report.when == "call":
            if report.passed:
                self.passed += 1
            elif report.failed:
                self.failed += 1

def run_tests():
    collector = ResultCollector()
    pytest.main(["tests"], plugins=[collector])
    print(f"\nToplam Başarılı: {collector.passed}")
    print(f"Toplam Başarısız: {collector.failed}")
    
    user_score = (collector.passed / (collector.passed + collector.failed)) * 100
    print(round(user_score, 2))
    
    url = "https://edugen-backend-487d2168bc6c.herokuapp.com/projectLog/"
    payload = {
        "user_id": 233,
        "project_id": 38,
        "user_score": round(user_score, 2),
        "is_auto": False
    }
    headers = {
        "Content-Type": "application/json"
    }
    send_post_request(url, payload, headers)

if __name__ == "__main__":
    run_tests()
