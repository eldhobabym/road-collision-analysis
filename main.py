import os
from dotenv import load_dotenv

from ingest_bronze import IngestData

# from silver_layer import ProcessingSilverLayer
from silver_layer import ProcessingSilverLayer


def main():
    IngestData()
    ProcessingSilverLayer()


if __name__ == "__main__":
    # Execute the main function only if this script is run directly, not if it's imported as a module
    main()
