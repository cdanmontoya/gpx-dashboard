from geopy.extra.rate_limiter import RateLimiter

from gpx_dashboard.pipelines.data_enrichment.nodes import get_geocoder


def test_get_geocoder():
    geocoder = get_geocoder()

    assert isinstance(geocoder, RateLimiter)
