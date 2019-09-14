from invoke import task


@task
def test(ctx):
    ctx.run("pytest -s -x -vv --tb=short --color=yes tests")


@task
def lint_black(ctx):
    ctx.run("black .")


@task
def lint_pylint(ctx):
    ctx.run("pylint dts")
    ctx.run("pylint tests")


@task(pre=[lint_black, lint_pylint])
def lint(ctx):
    pass
