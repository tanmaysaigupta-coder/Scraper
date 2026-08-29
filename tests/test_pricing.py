from src.crawler.pricing import classify_pricing
from src.schemas import PricingModel


def test_freemium_free_plan_plus_paid():
    assert classify_pricing("Free plan available. Pro plan $12/month.") is PricingModel.FREEMIUM


def test_enterprise_contact_sales_only():
    assert classify_pricing("Pricing: contact us for sales. Custom pricing for teams.") is PricingModel.ENTERPRISE


def test_paid_explicit_price_no_free():
    assert classify_pricing("Starting at $29 per month. 14-day trial.") is PricingModel.PAID


def test_free_open_source():
    assert classify_pricing("Completely free and open-source. No cost, ever.") is PricingModel.FREE


def test_none_when_no_signal():
    assert classify_pricing("An AI tool that summarizes your meetings.") is None
    assert classify_pricing("") is None
