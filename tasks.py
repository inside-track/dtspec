from invoke import task


@task
def test(ctx):
    ctx.run("pytest -s -x -vv --tb=short --color=yes tests")


@task
def lint_black(ctx, check=False):
    ctx.run(f"black {'--check' if check else ''} tasks.py")
    ctx.run(f"black {'--check' if check else ''} dtspec")
    ctx.run(f"black {'--check' if check else ''} tests")


@task
def lint_pylint(ctx):
    ctx.run("pylint dtspec")
    ctx.run("pylint tests")


@task
def lint(ctx, check=False):
    lint_black(ctx, check=check)
    lint_pylint(ctx)


@task
def package(ctx):
    """
    Package distribution to upload to PyPI
    """
    ctx.run("rm -rf dist")
    ctx.run("python setup.py sdist")


@task
def package_deploy(ctx):
    """
    Deploy package to PyPI
    """
    ctx.run("twine upload dist/*")
