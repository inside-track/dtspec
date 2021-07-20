import os
import subprocess
import shlex

from dtspec.log import LOG


class RunCommandError(Exception):
    pass


def run_command(command, env=None):
    "Runs a shell command and streams output to the log"

    env = env or {}

    with subprocess.Popen(
        shlex.split(command),
        env={**os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ) as proc:
        while True:
            output = proc.stdout.readline().decode("utf-8")
            if len(output) == 0 and proc.poll() is not None:
                break
            if output:
                LOG.info(output.strip())

        return_code = proc.poll()
    if return_code != 0:
        raise RunCommandError(
            "Error running shell command, please see log for details."
        )


class DbtRunError(Exception):
    pass


def run_dbt(
    cmd="run",
    profiles_dir=None,
    target="dev",
    models=None,
    exclude=None,
    full_refresh=False,
    env=None,
    partial_parse=False,
):
    "Construct common dbt parameters and runs dbt in a shell"

    profiles_dir = profiles_dir or os.environ.get("DBT_PROFILES_DIR", "~/.dbt/")

    env = env or {}

    models_cmd = ""
    if models:
        models_cmd = f"--models {models}"

    exclude_cmd = ""
    if exclude:
        exclude_cmd = f"--exclude {exclude}"

    full_refresh_cmd = ""
    if full_refresh:
        full_refresh_cmd = "--full-refresh"

    partial_parse_cmd = ""
    if partial_parse:
        partial_parse_cmd = "--partial-parse"

    shell_cmd = f"dbt {partial_parse_cmd} {cmd} --profiles-dir={profiles_dir} --target={target} {full_refresh_cmd} {models_cmd} {exclude_cmd}"
    LOG.info("Running dbt via: %s", shell_cmd)

    try:
        run_command(shell_cmd, env=env)
    except RunCommandError as err:
        raise DbtRunError(
            f"dbt failed to {cmd} successfully, please see log for details"
        ) from err
