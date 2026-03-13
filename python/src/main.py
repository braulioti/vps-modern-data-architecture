"""
Script that reads the .env file and uses the FTP_DATASUS attribute.
"""
import logging
import shutil
from pathlib import Path

from config import EnvLoader
from config.logging_config import setup_logging
from integration import AWSIntegration, DatasusIntegration


S3_RAW_SIH_PREFIX = "raw/sih/"
S3_RAW_IBGE_MUNICIPIOS_PREFIX = "raw/ibge-municipios/"
S3_RAW_IBGE_UF_PREFIX = "raw/ibge-uf/"
S3_RAW_SIGTAP_PREFIX = "raw/sigtap/"
S3_RAW_CID10_PREFIX = "raw/cid10/"
S3_RAW_NACIONAL_PREFIX = "raw/nacional/"
SIH_GLUE_JOB_NAME = "sih-sus-job"


def _subpath_from_s3_prefix(prefix: str) -> str:
    """S3 prefix 'raw/sih/' -> local subpath 'sih' (same structure as S3 but without raw/)."""
    return prefix.replace("raw/", "").strip("/")


def upload_csv_to_s3(
    csv_path: str | None,
    bucket: str | None,
    prefix: str = S3_RAW_SIH_PREFIX,
) -> None:
    """Upload all CSV files from the given directory to the S3 bucket under the given prefix."""
    if not csv_path or not bucket:
        return
    logger = logging.getLogger(__name__)
    csv_dir = _resolve_path(csv_path)
    if not csv_dir.is_dir():
        return
    aws = AWSIntegration()
    for csv_file in csv_dir.glob("*.csv"):
        key = f"{prefix}{csv_file.name}"
        uri = aws.send_to_s3_bucket(
            bucket, key, str(csv_file), content_type="text/csv"
        )
        logger.info("uploaded_csv_to_s3 uri=%s prefix=%s", uri, prefix)


def _resolve_path(path_str: str, base: Path | None = None) -> Path:
    """Resolve path: if relative, resolve against base (default: directory of main.py)."""
    p = Path(path_str)
    if p.is_absolute():
        return p
    base = base or Path(__file__).resolve().parent
    return (base / path_str).resolve()


def copy_csv_to_local_data(
    csv_path: str | None,
    data_base: str,
    s3_prefix: str,
) -> None:
    """Copy all CSV files from csv_path to data_base/<subpath>/ (subpath = s3_prefix without 'raw/')."""
    logger = logging.getLogger(__name__)
    if not csv_path or not data_base or not data_base.strip():
        logger.debug("copy_csv_to_local_data skipped csv_path=%s data_base=%s", csv_path, data_base)
        return
    csv_dir = _resolve_path(csv_path)
    if not csv_dir.is_dir():
        logger.warning("copy_csv_to_local_data source_not_dir path=%s", csv_dir)
        return
    subpath = _subpath_from_s3_prefix(s3_prefix)
    dest_dir = _resolve_path(data_base) / subpath
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = list(csv_dir.glob("*.csv"))
    if not files:
        logger.debug("copy_csv_to_local_data no_csv_files source=%s subpath=%s", csv_dir, subpath)
        return
    for csv_file in files:
        dest = dest_dir / csv_file.name
        try:
            shutil.copy2(csv_file, dest)
            logger.info("copy_csv_to_local path=%s subpath=%s", dest, subpath)
        except OSError as e:
            logger.warning("copy_csv_to_local_failed path=%s error=%s", dest, e)
    logger.info("copy_csv_to_local_data done subpath=%s count=%s dest_dir=%s", subpath, len(files), dest_dir)


def upload_ibge_csv_to_s3(loader: EnvLoader) -> None:
    """If process_ibge is enabled, upload IBGE municipalities and UF CSV folders to S3 and copy to local data."""
    if not loader.process_ibge:
        return
    upload_csv_to_s3(
        loader.csv_ibge_municipios_folder,
        loader.aws_s3_bucket,
        prefix=S3_RAW_IBGE_MUNICIPIOS_PREFIX,
    )
    copy_csv_to_local_data(
        loader.csv_ibge_municipios_folder,
        loader.data_path,
        S3_RAW_IBGE_MUNICIPIOS_PREFIX,
    )
    upload_csv_to_s3(
        loader.csv_ibge_uf_folder,
        loader.aws_s3_bucket,
        prefix=S3_RAW_IBGE_UF_PREFIX,
    )
    copy_csv_to_local_data(
        loader.csv_ibge_uf_folder,
        loader.data_path,
        S3_RAW_IBGE_UF_PREFIX,
    )


def main() -> None:
    loader = EnvLoader()
    loader.load()

    setup_logging(loader.log_file, loader.log_level)
    logger = logging.getLogger(__name__)

    logger.info("starting_main ftp_datasus=%s", loader.ftp_datasus)

    if loader.ftp_datasus:
        datasus = DatasusIntegration(loader)
        datasus.process_datasus()
        upload_csv_to_s3(loader.temp_csv_path, loader.aws_s3_bucket)
        copy_csv_to_local_data(
            loader.temp_csv_path, loader.data_path, S3_RAW_SIH_PREFIX
        )
        status_list = datasus.get_status_download_sih()
        if any(s.status == "success" for s in status_list):
            aws = AWSIntegration()
            run_id = aws.call_job_glue(SIH_GLUE_JOB_NAME)
            logger.info(
                "glue_job_started job_name=%s run_id=%s", SIH_GLUE_JOB_NAME, run_id
            )
        else:
            logger.warning(
                "no_sih_download_success glue_job_not_started job_name=%s",
                SIH_GLUE_JOB_NAME,
            )
        upload_csv_to_s3(
            loader.csv_ibge_sigtap_folder,
            loader.aws_s3_bucket,
            prefix=S3_RAW_SIGTAP_PREFIX,
        )
        copy_csv_to_local_data(
            loader.csv_ibge_sigtap_folder,
            loader.data_path,
            S3_RAW_SIGTAP_PREFIX,
        )
        upload_csv_to_s3(
            loader.csv_ibge_cid10_folder,
            loader.aws_s3_bucket,
            prefix=S3_RAW_CID10_PREFIX,
        )
        copy_csv_to_local_data(
            loader.csv_ibge_cid10_folder,
            loader.data_path,
            S3_RAW_CID10_PREFIX,
        )
        upload_csv_to_s3(
            loader.csv_nacional_folder,
            loader.aws_s3_bucket,
            prefix=S3_RAW_NACIONAL_PREFIX,
        )
        copy_csv_to_local_data(
            loader.csv_nacional_folder,
            loader.data_path,
            S3_RAW_NACIONAL_PREFIX,
        )
        upload_ibge_csv_to_s3(loader)


if __name__ == "__main__":
    main()
