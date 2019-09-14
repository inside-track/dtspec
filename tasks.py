from invoke import task


@task
def test(ctx):
    ctx.run("pytest -s -x -vv --tb=short --color=yes tests")


@task
def lint_black(ctx, check=False):

    ctx.run(f"black {'--check' if check else ''} .")


@task
def lint_pylint(ctx):
    ctx.run("pylint dts")
    ctx.run("pylint tests")


@task
def lint(ctx, check=False):
    lint_black(ctx, check=check)
    lint_pylint(ctx)
