import yaml
import sys


def clean_exchange():
    with open("config/config.yaml", "r") as f:
        data = yaml.safe_load(f)
    if "exchange" in data:
        del data["exchange"]
    with open("config/config.yaml", "w") as f:
        yaml.dump(data, f)


if __name__ == "__main__":
    clean_exchange()
