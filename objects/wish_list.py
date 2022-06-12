from datetime import timedelta

class WishList(object):
    def __init__(self,
                 upper_days_old=None,
                 lower_days_old=None,
                 post_unique=None,
                 add_ads=None,
                 ads=None,
                 ads_position=None,
                 add_hashtag=None,
                 column_in_post_obj=None,
                 cleared_urls=None,
                 time_delay_upper=None,
                 time_delay_lower=None):

        self.upper_days_old = upper_days_old
        self.lower_days_old = lower_days_old
        assert isinstance(upper_days_old, int)
        assert isinstance(lower_days_old, int)
        assert lower_days_old > upper_days_old
        assert isinstance(post_unique, bool)
        self.post_unique = post_unique
        assert isinstance(add_ads, bool)
        self.add_ads = add_ads
        if add_ads:
            assert isinstance(ads, str) or isinstance(ads, list)
        self.ads = ads
        if ads_position is not None:
            ads_position in ["First", "Second"]
        self.ads_position = ads_position
        assert isinstance(add_hashtag, bool)
        self.add_hashtag = add_hashtag
        if post_unique:
            assert isinstance(column_in_post_obj, str)
        assert isinstance(cleared_urls, list) or cleared_urls is None
        self.cleared_urls = cleared_urls
        assert isinstance(time_delay_upper, timedelta)
        self.time_delay_upper = time_delay_upper
        assert isinstance(time_delay_lower, timedelta)
        assert time_delay_lower < time_delay_upper
        self.time_delay_lower = time_delay_lower
