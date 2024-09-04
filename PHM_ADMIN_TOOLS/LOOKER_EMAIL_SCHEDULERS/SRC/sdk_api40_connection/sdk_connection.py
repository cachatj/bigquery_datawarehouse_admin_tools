from os import getenv

from looker_sdk import api_settings, init40


class LookerApiSettings(api_settings.ApiSettings):
    """Creates a configuration to be used with the Looker API

    Args:
        api_settings (_type_): We are just passing base_url to it
    """

    def __init__(self, base_url):
        self.base_url = base_url
        super().__init__()

    def read_config(self) -> api_settings.SettingsConfig:
        config = super().read_config()
        config["base_url"] = self.base_url
        # config["client_id"] = ""
        with open(getenv("LOOKER_PHARMA_CLIENT"), encoding="utf-8") as f:
            config["client_id"] = f.read()
        # config["client_id"] = getenv("LOOKERSDK_CLIENT_ID")
        # config["client_secret"] = getenv("LOOKERSDK_CLIENT_SECRET")
        with open(getenv("LOOKER_PHARMA_SECRET"), encoding="utf-8") as f:
            config["client_secret"] = f.read()
        config["verify_ssl"] = "false"
        config["timeout"] = 5000
        return config


class SDKConnection:
    """Create the Looker SDK connection"""

    def __init__(self, base_url):
        self.base_url = base_url
        self.sdk = None

    def sdk_connection(self):
        self.sdk = init40(config_settings=LookerApiSettings(self.base_url))

    def get_sdk_connection(self):
        if self.sdk is None:
            self.sdk_connection()
        return self.sdk
