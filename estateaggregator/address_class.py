import numpy as np

class Address(object):
    """Class for encapsulating address data point and cleaning/verifying it.
    """
    def __init__(self,house_number,street,city,state,zipcode):
        self.house_number = house_number
	self.street = street
	self.city = city
	self.state = state
	self.zipcode = zipcode

    @classmethod
    def from_address_dictionary(cls,address_dict):
        return(cls(address_dict['house_number'],address_dict['road'],
	           address_dict['city'],address_dict['state'],address_dict['postcode']))
        
