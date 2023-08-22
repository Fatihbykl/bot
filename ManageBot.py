

class ManageBot:
    instance = None

    def __new__(cls, db):
        """Code for Singleton"""

        if not isinstance(cls.instance, cls):
            cls.instance = super(ManageBot, cls).__new__(cls)
        return cls.instance

    def __init__(self, db):
        self.db = db

    def get_tradeable_coins(self):
        pass

    def start_bots(self):
        pass

