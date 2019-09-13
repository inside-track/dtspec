from invoke import task

@task
def test(ctx):
    ctx.run('pytest -s -x -vv --tb=short --color=yes tests')

@task
def lint_black(ctx):
    ctx.run('black dts')
    ctx.run('black tests')
