#! /usr/bin/env python

from argparse import Action, ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from copy import copy
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from json import dump as json_dump, loads as json_loads
from logging import getLogger, StreamHandler
from re import sub as re_sub
from typing import Any, Collection, Dict, List, Optional, Tuple
from urllib.parse import quote as urllib_quote
import os

from ntplib import NTPClient, NTPException  # type: ignore
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3 import Retry  # type: ignore


logger = getLogger(__name__)

_DEFAULT_CHANNELS = (
    "https://conda.anaconda.org/bioconda",
    "https://conda.anaconda.org/conda-forge",
    "https://repo.anaconda.com/pkgs/main",
    "https://repo.anaconda.com/pkgs/free",
    "https://repo.anaconda.com/pkgs/r",
)

_DEFAULT_SUBDIRS = (
    "noarch",
    "linux-64",
    "linux-aarch64",
    "linux-ppc64le",
    "osx-64",
    "osx-arm64",
    "win-64",
)


@dataclass
class OutputConfig:
    output_root: str
    indent: int = 0
    separators: Tuple[str, str] = (",", ":")
    trim_keys: Collection[str] = (
        "md5",
        "subdir",
        "license_family",
        "app_entry",
        "app_own_environment",
        "app_type",
        "arch",
        "icon",
        "platform",
        "summary",
        "type",
        "binstar",
        "has_prefix",
        "machine",
        "operatingsystem",
        "target-triplet",
    )


class FetchError(Exception):
    pass


def get_ntp_time() -> str:
    ntp_pool = (
        "0.pool.ntp.org",
        "1.pool.ntp.org",
        "2.pool.ntp.org",
        "3.pool.ntp.org",
    )
    client = NTPClient()
    for server in ntp_pool:
        try:
            response = client.request(server, version=4, timeout=2)
        except NTPException:
            continue
        time = datetime.fromtimestamp(response.tx_time)
        return time.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    raise FetchError("Could not get timestamp.")


class AppendDefaultAction(Action):
    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: Any,
        option_string: Optional[str] = None,
    ) -> None:
        items = getattr(namespace, self.dest, None)
        if items is self.default or items is None:
            items = []
        items.append(values)
        setattr(namespace, self.dest, items)


def cleanup_unneeded_info(
    repodata: Dict[str, Any],
    trim_keys: Collection[str],
) -> Dict[str, Any]:
    if not trim_keys:
        return repodata
    output_repodata = repodata.copy()
    for packages_key in ["packages", "packages.conda"]:
        if packages_key in repodata:
            output_repodata[packages_key] = {
                filename: {key: value for key, value in package.items() if key not in trim_keys}
                for filename, package in repodata[packages_key].items()
            }
    return output_repodata


def fetch_repodata(
    session: Session, timestamp: str, output_config: OutputConfig, channel: str, subdir: str
) -> Tuple[str, str, bool]:
    url_prefix = f"{channel}/{subdir}"
    url = f"{url_prefix}/repodata.json"
    logger.info(f"Fetching {url} ...")
    try:
        response = session.get(url)
        response.raise_for_status()
    except HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(
                "No repodata.json found for subdir %s in channel %s .", subdir, channel
            )
            return channel, subdir, False
        raise
    else:
        input_repodata = json_loads(response.text)
        output_repodata = cleanup_unneeded_info(input_repodata, output_config.trim_keys)
        output_subdir = re_sub("//", "%2F/", urllib_quote(url_prefix))
        output_dir = output_config.output_root + "/" + output_subdir
        os.makedirs(output_dir, exist_ok=True)
        output_file_path = f"{output_dir}/repodata.json"
        logger.info(f"Writing {output_file_path} ...")
        with open(output_file_path + ".time", "w") as output_timestamp_file:
            print(timestamp, file=output_timestamp_file)
        with open(output_file_path, "w") as output_file:
            json_dump(
                output_repodata,
                output_file,
                sort_keys=True,
                indent=output_config.indent,
                separators=output_config.separators,
            )
    return channel, subdir, True


def fetch(
    output_config: OutputConfig, channels: Tuple[str, ...], subdirs: Tuple[str, ...]
) -> None:
    with Session() as session:
        session.headers["User-Agent"] += " https://github.com/bioconda/bioconda-repodata"
        retry_config = Retry(
            total=5,
            backoff_factor=0.1,
            raise_on_status=True,
            raise_on_redirect=False,
        )
        adapter = HTTPAdapter(max_retries=retry_config)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        timestamp = get_ntp_time()
        with ProcessPoolExecutor() as executor:
            futures = []
            for subdir in subdirs:
                for channel in channels:
                    futures.append(
                        executor.submit(
                            fetch_repodata, session, timestamp, output_config, channel, subdir
                        ),
                    )
            unfetched_channels = set(channels)
            for future in as_completed(futures):
                channel, _, success = future.result()
                if success:
                    unfetched_channels -= {channel}
            if unfetched_channels:
                raise FetchError(
                    "No repodata.json found for any subdir in channels %s .",
                    unfetched_channels,
                )
    with open(f"{output_config.output_root}/.time", "w") as timestamp_file:
        print(timestamp, file=timestamp_file)


def parse_args(
    parser: ArgumentParser, argv: Optional[List[str]]
) -> Tuple[str, OutputConfig, Tuple[str, ...], Tuple[str, ...]]:
    args = parser.parse_args(argv)

    channels = tuple(map(str, args.channel))
    subdirs = tuple(map(str, args.subdirs))

    output_config = OutputConfig(
        output_root=args.output,
        indent=args.indent,
        separators=args.separators,
        trim_keys=set(args.trim_keys) if args.trim else {},
    )

    log_level: str = args.log_level.upper()

    return log_level, output_config, channels, subdirs


def get_argument_parser() -> ArgumentParser:
    parser = ArgumentParser(
        prog="fetch-repodata",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--channel",
        action=AppendDefaultAction,
        default=_DEFAULT_CHANNELS,
        help=f"Full channel URL.",
    )
    parser.add_argument(
        "--subdir",
        action=AppendDefaultAction,
        default=_DEFAULT_SUBDIRS,
        dest="subdirs",
        help="Subdir.",
    )
    parser.add_argument(
        "--output",
        default=f"{os.getcwd()}/repodata",
        help="Root of output directory. Defaults to ./repodata in current working directory.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=OutputConfig.indent,
        help="Indentation of JSON output files.",
    )
    parser.add_argument(
        "--separators",
        default=OutputConfig.separators,
        help="Separators used in JSON output files.",
    )
    parser.add_argument(
        "--trim",
        action="store_true",
        help=(
            "Remove redundant or seldomly used package metadata. "
            "Can be used with --trim-key to shrink output repodata.json files by ~15 %%."
        ),
    )
    parser.add_argument(
        "--trim-key",
        action=AppendDefaultAction,
        default=OutputConfig.trim_keys,
        dest="trim_keys",
        help="Package metadata key to remove. Only used if --trim is provided.",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
    )
    return parser


def main(argv: Optional[List[str]] = None) -> None:
    argument_parser = get_argument_parser()
    log_level, output_config, channels, subdirs = parse_args(argument_parser, argv)
    log_handler = StreamHandler()
    log_handler.setLevel(log_level)
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    fetch(output_config, channels, subdirs)


if __name__ == "__main__":
    main()
