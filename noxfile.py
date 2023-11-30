import os
import nox

os.environ.update({"PDM_IGNORE_SAVED_PYTHON": "1"})

@nox.session(python=['3.10', '3.11', '3.12'])
def tests(session):
    session.run_always('pdm', 'install', '-G', 'test', external=True)
    session.run('pytest')

# @nox.session
# def lint(session):
#     session.run_always('pdm', 'install', '-G', 'lint', external=True)
#     session.run('flake8', '--import-order-style', 'google')
