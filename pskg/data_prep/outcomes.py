from boto3 import client
import logging


class OutcomeMapper(object):
    __standard_outcomes = [
        "birth defect",
        "death",
        "disabled",
        "doctor visit",
        "er visit",
        "hospitalization",
        "incapacity",
        "in recovery",
        "life threatening",
        "other medically important condition",
        "prolonged hospitalization",
        "sequelae",
        "unknown",
        "recovered",
    ]
    __outcome_mapping = {}
    logger = logging.getLogger("pskg_loader.OutcomeMapper")

    @classmethod
    def show_mappings(cls, dataset=None):
        if dataset is not None:
            # Show specific mapping
            mapping = cls.__outcome_mapping.get(dataset)
            if mapping is not None:
                cls.logger.info(f"{dataset}")
                cls.logger.info(f"\n{mapping}")
            else:
                cls.logger.warn(f"No mapping defined for {dataset}")
        else:
            for k, v in cls.__outcome_mapping.items():
                # Show all mappings
                cls.logger.info("All Mappings:")
                cls.logger.info(f"Dataset: {k}")
                cls.logger.info(f"\n{v}")
        return

    @staticmethod
    def derive_outcomes(input_row, dataset, include_na=False):
        """
        Apply mapping to standard outcomes.  NOTE: input_row must
        contain columns for defined outcomes otherwise a key error
        will occur.
        """
        mapping = OutcomeMapper.__outcome_mapping[dataset]
        input_row = input_row.loc[mapping["dataset_outcomes"]]
        return ",".join(
            list(set(mapping[input_row.fillna(include_na).values]["standard_outcomes"]))
        )

    @classmethod
    def register_outcome_mapping(cls, dataset, mapping):
        """
        Register a new outcome mapping for a dataset.

        Parameters
        ----------
        dataset : string
            Name key for a dataset.

        mapping : pd.DataFrame
            Dataframe with 2 columns : standard_outcomes and dataset_outcomes. This
            Dataframe defines the mapping in a pair-wise manner. Repeating standard
            outcomes for multiple dataset outcomes is allowed but not vice-versa.
        """
        # TODO: Validate the provided mapping...
        cls.__outcome_mapping[dataset] = mapping
