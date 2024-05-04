import usaddress


class Address:
    STREET_LABELS = [
        "AddressNumberPrefix",
        "AddressNumber",
        "AddressNumberSuffix",
        "StreetNamePreModifier",
        "StreetNamePreDirectional",
        "StreetNamePreType",
        "StreetName",
        "StreetNamePostType",
        "StreetNamePostDirectional",
        "SubaddressType",
        "SubaddressIdentifier",
        "BuildingName",
        "OccupancyType",
        "OccupancyIdentifier",
        "CornerOf",
        "LandmarkName"
    ]

    def __init__(self, full_address: str):
        self.full_address: str = full_address
        self.street_name: str = ""
        self.city: str = ""
        self.state: str = ""
        self.zip: str = ""

        self._parse_address()

    @property
    def is_valid(self):
        return self.street_name and self.city and self.state and self.zip

    @staticmethod
    def _format_str(string: str):
        return string.strip().strip(",").title()

    def _parse_address(self):
        street_str = ""
        parsed_address = usaddress.parse(self.full_address)

        address_dict = {}
        for value, key in parsed_address:
            address_dict[key] = address_dict[key] + f" {value}" if address_dict.get(key) else value

        for label in self.STREET_LABELS:
            try:
                street_str += f"{address_dict[label].title()} "
            except KeyError:
                pass

        self.street_name = self._format_str(street_str)
        self.city = self._format_str(address_dict.get("PlaceName"))
        self.state = address_dict.get("StateName").upper()
        self.zip = address_dict.get("ZipCode")

    def __str__(self):
        return self.full_address

    def __repr__(self):
        return self.full_address

