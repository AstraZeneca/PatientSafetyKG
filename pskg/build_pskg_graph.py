#!/usr/bin/env python
# coding: utf-8

###
### PSKG Graph Loading Script
###
### Reads source data from S3 and APIs and populates an empty graph database.
###

### Imports

import argparse
import logging
import os
import time
from datetime import date, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from data_nodes import (
    az_exposure,
    cdc_exposure,
    eu_case,
    vaers_case,
    vaers_vaccine,
    eu_drug,
    geocoding,
    meddracq,
    meddra,
    case_group,
)
from data_edges import (
    vaers_case_administered_vaccine,
    vaers_case_reported_ae_meddra_term,
    eu_case_prescribed_medication,
    eu_case_administered_vaccine,
    eu_case_reported_ae_meddra_term,
    vaers_case_reported_from,
    eu_case_reported_from,
    geocoding_relationships,
    az_country_has_exposure,
    cdc_country_has_exposure,
    az_vaccine_has_exposure,
    cdc_vaccine_has_exposure,
    meddracq_links,
    meddra_ontology,
    case_group_case,
)
from data_prep import eudravigilance, vaers
from graph_objects import utils as gu

#
# Helper functions
#


def read_config(config_file):
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def get_meddracq_project_folder(cfg, root_folder=None):
    if os.environ.get("PYTHON_INSTALL_LAYOUT") == "amzn":
        meddracq_project_folder = (
            Path(os.environ.get("HOME")) / cfg["MedDRACQ"]["aibench"]
        )
    else:
        if root_folder is None:
            root_folder = Path("..") / ".."
        elif isinstance(root_folder, Path):
            pass
        else:
            root_folder = Path(root_folder)
        meddracq_project_folder = root_folder / cfg["MedDRACQ"]["local"]

    return meddracq_project_folder


def main(
    az_exposure_tag,
    eudra_dataset_tag,
    output_data_version,
    cfg,
    vaers_combined_file=None,
    vaers_limit_list=None,
    input_bucket=None,
    data_path=None,
    meddra_folder_path=None,
    output_path=None,
    output_bucket=None,
    output_key=None,
):
    """
    Top level load function.  Attempt to load all specified data and produce TSV files for loading into Neo4J.
    """

    logger.info(f"Starting import {datetime.now()}")
    logger.info(f"MedDRA: {cfg['MedDRA']['VERSION']}")
    logger.info(f"Eudravigilance: {eudra_dataset_tag}")
    logger.info(f"AZ Exposure: {az_exposure_tag}")
    if vaers_combined_file:
        logger.info(f"VAERS combined VAERS archive: {vaers_combined_file}")
    else:
        logger.info(f"Using individual VAERS archive files.")
    if vaers_limit_list is None:
        logger.info(f"VAERS: All available files")
    else:
        logger.info(f"VAERS: Limit to {vaers_limit_list}")

    logger.info(f"Output Version: {output_data_version}")

    if output_path:
        # Writing to local file system
        logger.info(f"Writing to local file system: {output_path}")
        output_manager = gu.ImportPoolManager(
            s3_output_bucket=None, s3_output_key=None, output_folder=output_path
        )
    else:
        # Writing to S3
        key = output_key or cfg["S3_Locations"]["S3_OUTPUT_KEY"]
        final_output_key = f"{key}/{output_data_version}"
        final_bucket = output_bucket or cfg["S3_Locations"]["S3_OUTPUT_BUCKET"]
        logger.info(f"Writing to S3: {final_bucket}/{final_output_key}")
        output_manager = gu.ImportPoolManager(
            s3_output_bucket=final_bucket,
            s3_output_key=final_output_key,
            output_folder=None,
        )

    if data_path:
        if isinstance(data_path, str):
            data_path = Path(data_path)
        logger.info(f"Reading from local files: {data_path}")
        input_bucket = None
        az_exposure_file_path = (
            data_path / f"{az_exposure_tag}-{cfg['AZ_Exposure']['FILE_BASE']}"
        )
        az_exposure_file_key = None
        cdc_exposure_file_path = data_path / cfg["CDC"]["FILE_NAME"]
        cdc_exposure_key = None
        country_file_path = data_path / cfg["Geocoding"]["CONTINENTS_FILE"]
        country_file_key = None
        continents_file_path = data_path / cfg["Geocoding"]["CONTINENTS_FILE"]
        continents_file_key = None
        all_vaers_file_key = None
        if vaers_combined_file is not None:
            all_vaers_file_path = data_path / vaers_combined_file
        else:
            all_vaers_file_path = None
    else:
        logger.info(f"Reading from S3 Bucket: {input_bucket}")
        az_exposure_file_path = None
        az_exposure_file_key = f"{cfg['AZ_Exposure']['KEY']}/{az_exposure_tag}-{cfg['AZ_Exposure']['FILE_BASE']}"
        cdc_exposure_file_path = None
        cdc_exposure_key = f"{cfg['CDC']['KEY']}/{cfg['CDC']['FILE_NAME']}"
        country_file_path = None
        country_file_key = (
            f"{cfg['Geocoding']['KEY']}/{cfg['Geocoding']['CONTINENTS_FILE']}"
        )
        continents_file_path = None
        continents_file_key = (
            f"{cfg['Geocoding']['KEY']}/{cfg['Geocoding']['CONTINENTS_FILE']}"
        )
        all_vaers_file_path = None
        all_vaers_file_key = f"{cfg['VAERS']['ALL_VAERS_KEY']}/{vaers_combined_file}"

    ####
    #### Organize VAERS source data
    ####
    if vaers_combined_file:
        vaers_components = vaers.get_components_from_combined(
            input_bucket=input_bucket,
            input_key=all_vaers_file_key,
            file_path=all_vaers_file_path,
        )
    else:
        vaers_components = vaers.get_components_from_individual(
            input_bucket=input_bucket,
            input_key=cfg["VAERS"]["VAERS_KEY"],
            folder_path=data_path,
        )

    if vaers_limit_list:
        # limit results
        print("vaers_components", vaers_components.columns)
        vaers_components = vaers_components[
            vaers_components["tag"].isin(vaers_limit_list)
        ]

    if data_path:
        vaers_vaccine_types_path = data_path / cfg["VAERS"]["VAERS_DESC_FILE"]
        vaers_vaccine_types_s3_key = None
    else:
        vaers_vaccine_types_s3_key = (
            f"{cfg['VAERS']['VAERS_DESC_KEY']}/{cfg['VAERS']['VAERS_DESC_FILE']}"
        )
        vaers_vaccine_types_path = None

    ####
    #### Organize EudraVigilance source data
    ####
    eu_files = eudravigilance.find_line_listing_files(
        input_bucket=input_bucket,
        data_set_tag=eudra_dataset_tag,
        prefix=cfg["EudraVigilance"]["KEY"],
        folder_path=data_path,
    )

    ####
    #### Create Nodes
    ####

    # Gather all Case Node information
    case_pool = gu.Pool(name="Cases", output_file=cfg["Nodes"]["CASE_FILENAME"])
    ev_source = cfg["EudraVigilance"]["EV_SOURCE"]
    logger.info(f"ev_source: {ev_source}")
    logger.info(f"Registering cases in {case_pool}")
    for _, r in (
        vaers_components[["key", "file_path", "tag"]].drop_duplicates().iterrows()
    ):
        case_pool.register(
            vaers_case.VaersCase(
                data_set_tag=r["tag"],
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
            )
        )

    for _, r in eu_files[["key", "file_path"]].iterrows():
        case_pool.register(
            eu_case.EudraVigilanceCase(
                data_set_tag=eu_cutoff_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,
            )
        )
    output_manager.register(case_pool)

    # Gather all Vaccine Node information
    vaccine_pool = gu.Pool(
        name="Vaccines", output_file=cfg["Nodes"]["VACCINE_FILENAME"]
    )
    logger.info(f"Registering vaccines in {vaccine_pool}")
    for _, r in (
        vaers_components[["file_path", "key", "tag"]].drop_duplicates().iterrows()
    ):
        vaccine_pool.register(
            vaers_vaccine.VaersVaccine(
                data_set_tag=r["tag"],
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                desc_s3_key=vaers_vaccine_types_s3_key,
                desc_file_path=vaers_vaccine_types_path,
            )
        )
    for _, r in eu_files[["key", "file_path"]].iterrows():
        vaccine_pool.register(
            eu_drug.EudraVigilanceVaccine(
                data_set_tag=eu_cutoff_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,
            )
        )
    output_manager.register(vaccine_pool)

    # Gather all Medication Node information
    # Note medications are only available in EV records currently
    medication_pool = gu.Pool(
        name="Medications", output_file=cfg["Nodes"]["MEDICATION_FILENAME"]
    )
    logger.info(f"Registering medications in {medication_pool}")
    for _, r in eu_files[["key", "file_path"]].iterrows():
        medication_pool.register(
            eu_drug.EudraVigilanceMedication(
                data_set_tag=eu_cutoff_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,
            )
        )
    output_manager.register(medication_pool)

    # Gather geocoding information
    country_pool = gu.Pool(
        name="Countries", output_file=cfg["Nodes"]["COUNTRY_FILENAME"]
    )
    logger.info(f"Registering countries in {country_pool}")
    country_pool.register(
        geocoding.Country(
            s3_bucket=input_bucket, s3_key=country_file_key, file_path=country_file_path
        )
    )
    output_manager.register(country_pool)

    continents_pool = gu.Pool(
        name="Continents", output_file=cfg["Nodes"]["CONTINENT_FILENAME"]
    )
    logger.info(f"Registering continents in {continents_pool}")
    continents_pool.register(
        geocoding.Continent(
            s3_bucket=input_bucket,
            s3_key=continents_file_key,
            file_path=continents_file_path,
        )
    )
    output_manager.register(continents_pool)

    # Gather all Exposure Node information
    exposure_pool = gu.Pool(
        name="Exposure", output_file=cfg["Nodes"]["EXPOSURE_FILENAME"]
    )
    logger.info(f"Registering exposure in {exposure_pool}")
    exposure_pool.register(
        az_exposure.AzExposure(
            s3_bucket=input_bucket,
            s3_key=az_exposure_file_key,
            file_path=az_exposure_file_path,
        )
    )
    exposure_pool.register(
        cdc_exposure.CDCExposure(
            s3_bucket=input_bucket,
            s3_key=cdc_exposure_key,
            file_path=cdc_exposure_file_path,
        )
    )
    output_manager.register(exposure_pool)

    # Gather MedDRA Nodes
    meddra_nodes_pool = gu.Pool(
        name="MedDRA", output_file=cfg["Nodes"]["MEDDRA_TERM_FILENAME"]
    )
    logger.info(f"Registering MedDRA Terms in {meddra_nodes_pool}")
    meddra_nodes_pool.register(
        meddra.MeddraTerm(
            s3_bucket=input_bucket,
            s3_key=f"{cfg['MedDRA']['KEY']}/{cfg['MedDRA']['VERSION']}",
            folder_path=meddra_folder_path,
        )
    )
    output_manager.register(meddra_nodes_pool)

    # Gather MedDRA Custom Query Node information
    meddra_cq_pool = gu.Pool(
        name="MedDRACQ", output_file=cfg["Nodes"]["MEDDRACQ_META_FILE"]
    )
    logger.info(f"Registering Meddra Custom Queries in {meddra_cq_pool}")

    meddracq_project_folder = get_meddracq_project_folder(cfg)
    meddra_cq_pool.register(meddracq.MeddraCq(file_path=meddracq_project_folder))

    logger.info(
        f"Registration complete: {meddra_cq_pool} (project folder: {meddracq_project_folder.resolve()})"
    )
    output_manager.register(meddra_cq_pool)

    # MedDRA SMQs
    meddra_smq_smq_pool = gu.Pool(
        name="MedDRA_SMQ", output_file=cfg["Nodes"]["MEDDRA_SMQ_FILENAME"]
    )
    logger.info(f"Registering MedDRA SMQs in {meddra_smq_smq_pool}")
    meddra_smq_smq_pool.register(
        meddra.MeddraSMQ(
            s3_bucket=input_bucket,
            s3_key=f"{cfg['MedDRA']['KEY']}/{cfg['MedDRA']['VERSION']}",
            folder_path=meddra_folder_path,
        )
    )
    output_manager.register(meddra_smq_smq_pool)

    # Case Groups
    case_group_pool = gu.Pool(
        name="CaseGroup", output_file=cfg["Nodes"]["CASEGROUP_FILENAME"]
    )
    logger.info(f"Registering EV Case Groups in {case_group_pool}")
    case_group_pool.register(case_group.EudraVigilanceCaseGroup())
    output_manager.register(case_group_pool)

    ####
    #### Create Edges
    ####
    case_meds_pool = gu.Pool(
        name="CasePrescribedMeds", output_file=cfg["Edges"]["PRESCRIBED_FILENAME"]
    )
    # NOTE: Currently no medication Rx information available in VAERS in a structured format.
    for _, r in eu_files[["key", "file_path"]].iterrows():
        case_meds_pool.register(
            eu_case_prescribed_medication.EudraVigilanceCasePrescribedMedication(
                data_set_tag=eu_cutoff_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,
            )
        )

    output_manager.register(case_meds_pool)

    case_admin_pool = gu.Pool(
        name="CaseAdminVaccines", output_file=cfg["Edges"]["ADMINISTERED_FILENAME"]
    )
    for _, r in (
        vaers_components[["file_path", "key", "tag"]].drop_duplicates().iterrows()
    ):
        case_admin_pool.register(
            vaers_case_administered_vaccine.VaersCaseAdministeredVaccine(
                data_set_tag=r["tag"],
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
            )
        )
    for _, r in eu_files[["key", "file_path"]].iterrows():
        case_admin_pool.register(
            eu_case_administered_vaccine.EudraVigilanceAdministeredVaccine(
                data_set_tag=eudra_dataset_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,
            )
        )
    output_manager.register(case_admin_pool)

    case_country_pool = gu.Pool(
        name="CaseCountries", output_file=cfg["Edges"]["REPORTED_FROM_FILENAME"]
    )
    for _, r in (
        vaers_components[["file_path", "key", "tag"]].drop_duplicates().iterrows()
    ):
        case_country_pool.register(
            vaers_case_reported_from.VaersCaseReportedFrom(
                data_set_tag=r["tag"],
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
            )
        )
    for _, r in eu_files[["key", "file_path"]].iterrows():
        case_country_pool.register(
            eu_case_reported_from.EudraVigilanceCaseReportedFrom(
                data_set_tag=eudra_dataset_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,                
            )
        )
    output_manager.register(case_country_pool)

    case_reported_ae_pool = gu.Pool(
        name="CaseReportedAEs", output_file=cfg["Edges"]["REPORTED_AE_FILENAME"]
    )
    for _, r in (
        vaers_components[["file_path", "key", "tag"]].drop_duplicates().iterrows()
    ):
        case_reported_ae_pool.register(
            vaers_case_reported_ae_meddra_term.VaersCaseReportedAEMeddraTerm(
                data_set_tag=r["tag"],
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
            )
        )
    for _, r in eu_files[["key", "file_path"]].iterrows():
        case_reported_ae_pool.register(
            eu_case_reported_ae_meddra_term.EudraVigilanceCaseReportedAEMeddraTerm(
                data_set_tag=eudra_dataset_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,                
            )
        )
    output_manager.register(case_reported_ae_pool)

    country_continents = gu.Pool(
        name="CountriesInContinent",
        output_file=cfg["Edges"]["COUNTRY_IN_CONTINENT_FILENAME"],
    )
    country_continents.register(
        geocoding_relationships.CountryInContinent(
            s3_bucket=input_bucket,
            s3_key=continents_file_key,
            file_path=continents_file_path,
        )
    )
    output_manager.register(country_continents)

    ###
    ### Exposure Edges
    ###

    country_exposure = gu.Pool(
        name="CountryHasExposure",
        output_file=cfg["Edges"]["COUNTRY_HAS_EXPOSURE_FILENAME"],
    )
    country_exposure.register(
        cdc_country_has_exposure.CDCCountryHasExposure(
            s3_bucket=input_bucket,
            s3_key=cdc_exposure_key,
            file_path=cdc_exposure_file_path,
        )
    )
    country_exposure.register(
        az_country_has_exposure.AZCountryHasExposure(
            s3_bucket=input_bucket,
            s3_key=az_exposure_file_key,
            file_path=az_exposure_file_path,
            s3_country_ref_key=country_file_key,
            country_ref_file_path=country_file_path,
        )
    )
    output_manager.register(country_exposure)

    vaccine_exposure = gu.Pool(
        name="VaccineHasExposure",
        output_file=cfg["Edges"]["VACCINE_HAS_EXPOSURE_FILENAME"],
    )
    vaccine_exposure.register(
        cdc_vaccine_has_exposure.CDCVaccineHasExposure(
            s3_bucket=input_bucket,
            s3_key=cdc_exposure_key,
            file_path=cdc_exposure_file_path,
        )
    )
    vaccine_exposure.register(
        az_vaccine_has_exposure.AZVaccineHasExposure(
            s3_bucket=input_bucket,
            s3_key=az_exposure_file_key,
            file_path=az_exposure_file_path,
        )
    )
    output_manager.register(vaccine_exposure)

    ###
    ### Meddra Custom Queries
    meddracq_links_pool = gu.Pool(
        "MeddraCqLinks", output_file=cfg["Edges"]["MEDDRACQ_LINKS_FILE"]
    )
    meddracq_links_pool.register(
        meddracq_links.MeddraCqLink(file_path=meddracq_project_folder)
    )
    output_manager.register(meddracq_links_pool)

    ###
    ### MedDRA Ontology
    meddra_ontology_pool = gu.Pool(
        "MeddraOntology", output_file=cfg["Edges"]["MEDDRA_ONTOLOGY_FILENAME"]
    )
    meddra_key_version = f"{cfg['MedDRA']['KEY']}/{cfg['MedDRA']['VERSION']}"
    meddra_ontology_pool.register(
        meddra_ontology.MeddraOntology(
            s3_bucket=input_bucket,
            s3_key=meddra_key_version,
            folder_path=meddra_folder_path,
        )
    )
    output_manager.register(meddra_ontology_pool)

    ###
    ### MedDRA SMQ Links
    meddra_smq_smq_pool = gu.Pool(
        "MeddraSMQtoSMQ", output_file=cfg["Edges"]["MEDDRA_SMQ_SMQ_LINK_FILENAME"]
    )
    meddra_smq_smq_pool.register(
        meddra_ontology.MeddraSMQContainsTerm(
            smq=True,
            s3_bucket=input_bucket,
            s3_key=meddra_key_version,
            folder_path=meddra_folder_path,
        )
    )
    output_manager.register(meddra_smq_smq_pool)

    meddra_smq_term_pool = gu.Pool(
        "MeddraSMQtoPT", output_file=cfg["Edges"]["MEDDRA_SMQ_TERM_LINK_FILENAME"]
    )
    meddra_smq_term_pool.register(
        meddra_ontology.MeddraSMQContainsTerm(
            smq=False,
            s3_bucket=input_bucket,
            s3_key=meddra_key_version,
            folder_path=meddra_folder_path,
        )
    )
    output_manager.register(meddra_smq_term_pool)

    ###
    ### Case Groups (CaseContains.tsv)
    case_group_contains_pool = gu.Pool(
        name="CaseGroupLinks", output_file=cfg["Edges"]["CONTAINS_CASE_FILENAME"]
    )

    for _, r in eu_files[["key", "file_path"]].iterrows():
        case_group_contains_pool.register(
            case_group_case.EudraVigilanceCaseGroupCase(
                data_set_tag=eudra_dataset_tag,
                s3_bucket=input_bucket,
                s3_key=r["key"],
                file_path=r["file_path"],
                ev_source=ev_source,                
            )
        )
    output_manager.register(case_group_contains_pool)

    ###
    ### CREATE ALL LOAD FILES
    ###
    output_manager.create_output()

    if output_path:
        logger.info(f"Output ready in local folder: {output_path}")
    else:
        logger.info(f"Output ready in S3 bucket: {final_bucket}/{final_output_key}")
        logger.info(
            f"   aws s3 cp --recursive s3://{final_bucket}/{final_output_key} data"
        )


###
##############################################################################################
###

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_path",
        default=None,
        help="Path to locally available source data files, overrides data_bucket",
    )

    parser.add_argument(
        "--input_bucket",
        help="S3 bucket where data files can be found, this option is overriden by data_path, if supplied",
        default=None,
    )

    parser.add_argument(
        "--eudravigilance",
        help="Cutoff date for EudraVigilance files, given as ddmmyyyy format (e.g. 10Oct2021)",
        metavar="ddMMMyyyy",
        required=True,
    )

    parser.add_argument(
        "--az_exposure",
        help="Exposure file version, given as YYYY-MM-DD (e.g. 2021-10-18)",
        metavar="yyyy-mm-dd",
        required=True,
    )

    parser.add_argument(
        "--meddra_folder_path",
        default=None,
        help="Path to local folder containing MedDRA files.",
    )

    parser.add_argument(
        "--vaers_combined",
        help="Use single VAERS archive labeled with this date, given as YYYY-MM-DD (e.g. 2021-10-18)",
        metavar="yyyy-mm-dd",
        required=False,
    )

    parser.add_argument(
        "-vf",
        "--vaers_filter",
        nargs="+",
        help="Optional set of VAERS data tags to process, when omitted all VAERS data are processed",
        required=False,
    )

    parser.add_argument(
        "--output_path",
        default=None,
        help="Path to local output location, overrides output_bucket",
    )

    parser.add_argument(
        "--output_bucket",
        help="S3 output bucket location",
        default=None,
    )

    parser.add_argument(
        "--output_key",
        help="S3 output bucket location",
        default=None,
    )

    parser.add_argument(
        "--output_data_version",
        help="Data version for output",
        required=True,
        metavar="YYYYMMDD",
    )

    parser.add_argument(
        "--config", help="Specify configuration file (YAML)", default="config.yml"
    )

    parser.add_argument(
        "--quiet",
        default=False,
        action="store_true",
        help="Limit messages printed to the console",
    )

    parsed_args = parser.parse_args()

    try:
        # Check EudraVigilance cutoff tag
        datetime.strptime(parsed_args.eudravigilance, "%d%b%Y")
    except Exception:
        raise ValueError("Supplied EudraVigilance cutoff is not in ddMMMYYYY format.")
    eu_cutoff_tag = parsed_args.eudravigilance

    try:
        # Check AZ Exposure tag
        datetime.strptime(parsed_args.az_exposure, "%Y-%m-%d")
    except Exception:
        raise ValueError("Supplied EudraVigilance cutoff is not in YYYY-MM-DD format.")
    az_exposure_tag = parsed_args.az_exposure

    if parsed_args.vaers_combined != None:
        try:
            # Check AZ Exposure tag
            datetime.strptime(parsed_args.vaers_combined, "%Y-%m-%d")
        except Exception:
            raise ValueError(
                "Supplied VAERS combined file is not in YYYY-MM-DD format."
            )
        vaers_combined = parsed_args.vaers_combined
    else:
        vaers_combined = None

    if parsed_args.quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO

    ###
    ### Attempt to read in configuration file
    ###
    if not parsed_args.config:
        raise ValueError("Configuration file not set (use --config)")

    config_path = Path(parsed_args.config)
    if not config_path.exists():
        raise ValueError(
            f"Configuration {config_path.resolve()} not found (use --config)"
        )

    cfg = read_config(config_path.resolve())

    project_root = Path("..")  # project path
    root_folder = project_root / ".."  # folder containing project and other repos

    ###
    ### Configuration Tests
    ###

    # MedDRA custom query project
    meddra_project_folder = get_meddracq_project_folder(cfg)
    if not meddra_project_folder.exists():
        raise ValueError(
            f"Custom query folder {meddra_project_folder} not found, this must be cloned from BitBucket."
        )

    ###
    ### Set up logging
    ###
    (project_root / cfg["Logging"]["LOG_ROOT"]).mkdir(exist_ok=True, parents=True)

    logger = logging.getLogger("pskg_loader")
    logger.propagate = False
    logger.setLevel(logging.INFO)

    log_file = project_root / cfg["Logging"]["LOG_ROOT"] / cfg["Logging"]["LOG_FILE"]
    fh = TimedRotatingFileHandler(
        log_file.resolve(), when="D", interval=1, backupCount=5
    )
    fh.setLevel(logging.INFO)
    fh_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s: %(message)s"
    )
    fh.setFormatter(fh_formatter)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s: %(message)s"
    )
    ch.setFormatter(ch_formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    print("Logging to:", log_file.resolve())

    logger.info("Begin PSKG Import Processing...")
    start_time = time.time()

    try:
        main(
            az_exposure_tag=az_exposure_tag,
            eudra_dataset_tag=eu_cutoff_tag,
            vaers_combined_file=parsed_args.vaers_combined,
            vaers_limit_list=parsed_args.vaers_filter,
            input_bucket=parsed_args.input_bucket
            or cfg["S3_Locations"]["S3_INPUT_BUCKET"],
            data_path=parsed_args.data_path,
            meddra_folder_path=parsed_args.meddra_folder_path,
            output_path=parsed_args.output_path,
            output_bucket=parsed_args.output_bucket
            or cfg["S3_Locations"]["S3_OUTPUT_BUCKET"],
            output_key=parsed_args.output_key or cfg["S3_Locations"]["S3_OUTPUT_KEY"],
            output_data_version=parsed_args.output_data_version,
            cfg=cfg,
        )
    except Exception as x:
        logger.exception("PSKG Processing Error, details:")
        raise
    finally:
        end_time = time.time()

        logger.info(
            f"Completed PSKG Import Processing ({(end_time - start_time) / 60.0:.2f} minutes)."
        )
