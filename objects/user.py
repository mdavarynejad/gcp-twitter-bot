from objects.wish_list import WishList

class User(object):
    def __init__(self, user_name=None, wish_list=None):
        assert isinstance(user_name, str)
        self.user_name=user_name
        assert isinstance(wish_list, WishList)
        self.wish_list = wish_list
