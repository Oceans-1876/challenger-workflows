import json
import logging
import re
import subprocess
import sys
from typing import Dict, List, Optional

logger = logging.getLogger("GNames")

SUPPORTED_GNAMES_VERSIONS = {"gnfinder": 1.0, "gnverifier": 1.0}


class GNames:
    def __init__(self) -> None:
        self.app_versions: Dict[str, str] = {}
        self.check_gnames_app("gnfinder")
        self.check_gnames_app("gnverifier")
        self.species_cache: Dict[str, dict] = {}

    def check_gnames_app(self, app_name: str) -> None:
        """
        Check if the given gnames app exists on the system
        and its version is not less than min_version.
        If the app does not exist terminate the process.
        If the version is not satisfied, log a warning.

        Parameters
        ----------
        app_name: names of a Global Names (gnames) app

        Returns
        -------
        version of the existing gnames app
        """
        min_version = SUPPORTED_GNAMES_VERSIONS[app_name]

        try:
            version_text = subprocess.run(
                [app_name, "-V"], check=True, capture_output=True
            ).stdout.decode("utf-8")
            version = re.search(r"version: v(\d+).(\d+)", version_text)
            if version:
                version_number = float(f"{version.groups()[0]}.{version.groups()[1]}")
                if version_number < min_version:
                    logger.warning(
                        f"You have {app_name} version {version_number}. "
                        f"The script is tested with {app_name} v{min_version}. "
                        f"The calls to {app_name} might not work as expected."
                    )
                self.app_versions[app_name] = version_text.strip().split("\n")[0]
            else:
                sys.exit(
                    f"Could not get {app_name} version. "
                    f"The script is tested with {app_name} v{min_version}. "
                    "Make sure you have the right version on your system."
                )

        except FileNotFoundError:
            sys.exit(f"{app_name} is missing")
        except subprocess.CalledProcessError:
            sys.exit(
                f"The script is tested with {app_name} v{min_version}. "
                "Make sure you have the right version on your system."
            )

    def extract(self, text: str) -> List[dict]:
        # Use gnfinder to parse species from text without verification
        with subprocess.Popen(["echo", text], stdout=subprocess.PIPE) as echo_proc:
            with subprocess.Popen(
                ["gnfinder", "-f", "compact", "-w", "2"],
                stdin=echo_proc.stdout,
                stdout=subprocess.PIPE,
            ) as gnfinder_proc:
                if gnfinder_proc.stdout:
                    return json.loads(gnfinder_proc.stdout.read())["names"] or []
        return []

    def verify(self, species_name: str, sources: Optional[List[str]] = None) -> dict:
        if species_name in self.species_cache:
            return self.species_cache[species_name]

        cmd = ["gnverifier", "-f", "compact", species_name]
        if sources:
            cmd.extend(["-s", ",".join(sources)])

        verified_species = {}

        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
        ) as gnverifier_proc:
            if gnverifier_proc.stdout:
                result = gnverifier_proc.stdout.read()
                try:
                    verified_species = json.loads(result)
                except json.decoder.JSONDecodeError:
                    logger.warning(f"Could not verify {species_name}")

                self.species_cache[species_name] = verified_species

        return verified_species
