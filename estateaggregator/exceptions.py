class NotInZillowDatabase(Exception):
    """Raised when address is not found in Zillow database.
    """
    def __init__(self,address):
        self.address=address

    def __str__(self):
        return("Address {} is not in Zillow Database".format(self.address))
