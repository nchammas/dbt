import os
import re
import shutil
import subprocess
import sys
import tempfile
import venv

from argparse import ArgumentParser
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from typing import Optional, Iterator, List


HOMEBREW_PYTHON = (3, 7)


# This should match the pattern in .bumpversion.cfg
VERSION_PATTERN = re.compile(
    r'(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)'
    r'((?P<prerelease>[a-z]+)(?P<num>\d+))?'
)


class Version:
    def __init__(self, raw: str) -> None:
        self.raw = raw
        groups = VERSION_PATTERN.match(self.raw).groupdict()

        self.major: int = int(groups['major'])
        self.minor: int = int(groups['minor'])
        self.patch: int = int(groups['patch'])
        self.prerelease: Optional[str] = None
        self.num: Optional[int] = None

        if groups['num'] is not None:
            self.prerelease = groups['prerelease']
            self.num = int(groups['num'])

    def __str__(self):
        return self.raw

    def homebrew_class_name(self) -> str:
        name = f'DbtAT{self.major}{self.minor}{self.patch}'
        if self.prerelease is not None and self.num is not None:
            name = f'{name}{self.prerelease.title()}{self.num}'
        return name

    def homebrew_filename(self):
        version_str = f'{self.major}.{self.minor}.{self.patch}'
        if self.prerelease is not None and self.num is not None:
            version_str = f'{version_str}-{self.prerelease}{self.num}'
        return f'dbt@{version_str}.rb'


@dataclass
class Arguments:
    version: Version
    part: str
    path: Path
    homebrew_path: Optional[str]
    homebrew_set_default: bool

    @classmethod
    def parse(cls) -> 'Arguments':
        parser = ArgumentParser(
            prog="Bump dbt's version, build packages"
        )
        parser.add_argument(
            'version',
            type=Version,
            help="The version to set",
        )
        parser.add_argument(
            'part',
            type=str,
            help="The part of the version to update",
        )
        parser.add_argument(
            '--path',
            type=Path,
            help='The path to the dbt repository',
            default=Path.cwd(),
        )
        parser.add_argument(
            '--homebrew-path',
            type=Path,
            help='The path to the dbt homebrew install',
            default=(Path.cwd() / '../homebrew-dbt'),
        )
        parser.add_argument(
            '--homebrew-set-default',
            action='store_true',
            help='If set, make this homebrew version the default',
        )
        parsed = parser.parse_args()
        return cls(
            version=parsed.version,
            part=parsed.part,
            path=parsed.path,
            homebrew_path=parsed.homebrew_path,
            homebrew_set_default=parsed.homebrew_set_default,
        )


_SUBPACKAGES = (
    'core',
    'plugins/postgres',
    'plugins/redshift',
    'plugins/bigquery',
    'plugins/snowflake',
)


def get_targets(path: Path):
    for subpath in _SUBPACKAGES:
        yield path / subpath


@contextmanager
def clean_dist(path: Path, make=False) -> Iterator[None]:
    dist_path = path / 'dist'
    if dist_path.exists():
        shutil.rmtree(dist_path)
    if make:
        os.makedirs(dist_path)
    yield dist_path


def set_version(path: Path, version: str, part: str):
    # bumpversion --commit --no-tag --new-version "${version}" "${port}"
    cmd = [
        'bumpversion', '--commit', '--no-tag', '--new-version', version, part
    ]
    subprocess.check_output(cmd, stderr=subprocess.STDOUT, cwd=path)
    print(f'bumped version to {version}')


def build_pypi_package(path: Path):
    # 'python setup.py sdist bdist_wheel'
    cmd = ['python', 'setup.py', 'sdist', 'bdist_wheel']
    subprocess.check_output(cmd, stderr=subprocess.STDOUT, cwd=path)


def _all_packages_in(path: Path) -> Iterator[Path]:
    for pattern in ('*.tar.gz', '*.whl'):
        yield from path.glob(pattern)


def build_pypi_packages(dbt_path: Path) -> Path:
    with clean_dist(dbt_path) as dist_path:
        sub_pkgs = []
        for path in _SUBPACKAGES:
            subpath = dbt_path / path
            with clean_dist(subpath) as sub_dist:
                build_pypi_package(subpath)
                sub_pkgs.extend(_all_packages_in(sub_dist))

        build_pypi_package(dbt_path)

        for package in sub_pkgs:
            shutil.copy(str(package), dist_path)

        print('built pypi packages')
        return dist_path


def upload_pypi_packages(dist_path: Path, *, test=True):
    # 'twine upload --repository-url https://test.pypi.org/legacy/'
    cmd = ['twine', 'upload']
    if test:
        # cmd.extend(['--repository-url', 'https://test.pypi.org/legacy/'])
        cmd.extend(['--repository', 'pypitest'])
    cmd.extend(str(p) for p in _all_packages_in(dist_path))
    print('uploading packages: {}'.format(' '.join(cmd)))
    subprocess.check_output(cmd, stderr=subprocess.STDOUT)


def make_venv(dbt_path: Path, dbt_version: str):
    build_path = dbt_path / 'build'
    venv_path = build_path / 'tmp-venv'
    os.makedirs(build_path, exist_ok=True)
    if venv_path.exists():
        shutil.rmtree(venv_path)

    env = PoetVirtualenv(dbt_version)
    env.create(venv_path)
    return venv_path


class PoetVirtualenv(venv.EnvBuilder):
    def __init__(self, dbt_version: Path) -> None:
        super().__init__(with_pip=True)
        self.dbt_version = dbt_version

    def post_setup(self, context):

        tmp = tempfile.mkdtemp()
        cmd = [
            context.env_exe, '-m', 'pip', 'install', '--upgrade',
            'homebrew-pypi-poet', f'dbt=={self.dbt_version}'
        ]
        print(f'installing homebrew-pypi-poet and dbt=={self.dbt_version}')
        try:
            result = subprocess.check_output(cmd, stderr=subprocess.STDOUT, cwd=tmp)
        finally:
            os.rmdir(tmp)
        print('done:')
        print(result.decode('utf-8'))


# resource "dbt" do
#   url "https://files.pythonhosted.org/packages/0b/eb/ca194c5a0a0d6a771e92752bfa8410456229e87e1743cd9775821bcf4679/dbt-0.15.2.tar.gz"
#   sha256 "7d29fb072a8ea7f04acae1519eade967f08b0f44ee7944169b446431dbff8d40"
# end

def _extract_parts(poet_data: str) -> Iterator[str]:
    # given the output of `poet -s dbt`, extract the url and the hash line
    lines = poet_data.split('\n')

    collecting = False
    for line in lines:
        line = line.strip()
        if not collecting:
            if line == 'resource "dbt" do':
                collecting = True
        else:
            if line == 'end':
                break
            yield line


DBT_HOMEBREW_FORMULA = '''
class {formula_name} < Formula
  include Language::Python::Virtualenv

  desc "Data build tool"
  homepage "https://github.com/fishtown-analytics/dbt"
  {url_data}
  {hash_data}
  version "{version}"
  revision 1

  depends_on "python3"
  depends_on "openssl"
  depends_on "postgresql"

  bottle do
    root_url "http://bottles.getdbt.com"
    # bottle hashes + versions go here
  end

  {dependencies}

  {trailer}
end
'''

DBT_HOMEBREW_TRAILER = '''
  def install
    venv = virtualenv_create(libexec, "python3")

    res = resources.map(&:name).to_set

    res.each do |r|
      venv.pip_install resource(r)
    end

    venv.pip_install_and_link buildpath

    bin.install_symlink "#{libexec}/bin/dbt" => "dbt"
  end

  test do
    (testpath/"dbt_project.yml").write("{name: 'test', version: '0.0.1', profile: 'default'}")
    (testpath/".dbt/profiles.yml").write(
      "{default: {outputs: {default: {type: 'postgres', threads: 1, host: 'localhost', port: 5432,
      user: 'root', pass: 'password', dbname: 'test', schema: 'test'}}, target: 'default'}}",
    )
    (testpath/"models/test.sql").write("select * from test")
    system "#{bin}/dbt", "test"
  end
'''


@dataclass
class DbtHomebrewTemplate:
    url_data: str
    hash_data: str
    dependencies: str
    version: Version

    def contents(self, versioned=True):
        if versioned:
            formula_name = self.version.homebrew_class_name()
        else:
            formula_name = 'Dbt'

        return DBT_HOMEBREW_FORMULA.format(
            formula_name=formula_name,
            url_data=self.url_data,
            hash_data=self.hash_data,
            version=self.version,
            dependencies=self.dependencies,
            trailer=DBT_HOMEBREW_TRAILER,
        )


def homebrew_formula_template(
    dbt_path: Path, version: Version
) -> DbtHomebrewTemplate:
    env_path = make_venv(dbt_path, version)
    print('done setting up virtualenv')
    poet = env_path / 'bin/poet'

    # get the dbt info
    output = subprocess.check_output([poet, '-s', 'dbt']).decode('utf-8')
    url_data, hash_data = _extract_parts(output)

    dependencies = subprocess.check_output([poet, '-r', 'dbt']).decode('utf-8')
    return DbtHomebrewTemplate(
        url_data=url_data,
        hash_data=hash_data,
        dependencies=dependencies,
        version=version,
    )


def create_homebrew_formula(
    template: DbtHomebrewTemplate, version: Version, homebrew_path: Path
) -> Path:
    formula_contents = template.contents(versioned=True)
    homebrew_formula_root = homebrew_path / 'Formula'

    homebrew_formula_path = homebrew_formula_root / version.homebrew_filename()
    if homebrew_formula_path.exists():
        raise ValueError('Homebrew formula path already exists!')
    homebrew_formula_path.write_text(formula_contents)
    return homebrew_formula_path


def homebrew_run_tests(formula_path: Path):
    subprocess.check_output(['brew', 'uninstall', '--force', formula_path])
    subprocess.check_output(['brew', 'install', formula_path])
    subprocess.check_output(['brew', 'test', 'dbt'])
    subprocess.check_output(['brew', 'audit', '--strict', 'dbt'])


def homebrew_commit_formula(
    formula_path: Path, version: str, homebrew_path: Path
):
    # add a commit for the new formula
    subprocess.check_output(
        ['git', 'add', formula_path],
        cwd=homebrew_path
    )
    subprocess.check_output(
        ['git', 'commit', '-m', f'add dbt@{version}'],
        cwd=homebrew_path
    )


def build_homebrew_package(
    dbt_path: Path,
    version: str,
    homebrew_path: Path,
    set_default: bool,
) -> Path:

    template = homebrew_formula_template(dbt_path=dbt_path, version=version)
    formula_path = create_homebrew_formula(
        template=template,
        version=version,
        homebrew_path=homebrew_path,
    )
    homebrew_run_tests(formula_path=formula_path)
    homebrew_commit_formula(
        formula_path=formula_path,
        version=version,
        homebrew_path=homebrew_path,
    )

    if set_default:
        assert False, 'should not be set!!!'
        set_default_homebrew_package(
            template=template,
            version=version,
            homebrew_path=homebrew_path,
        )

    return formula_path


def set_default_homebrew_package(
    template: DbtHomebrewTemplate, version: str, homebrew_path: Path
):
    # make the new formula into the main one
    default_path = homebrew_path / 'Formula/dbt.rb'
    os.remove(default_path)
    formula_contents = template.contents(versioned=False)
    default_path.write_text(formula_contents)
    subprocess.check_output(
        ['git', 'add', default_path],
        cwd=homebrew_path
    )
    subprocess.check_output(
        'git', 'commit', '-m', f'upgrade dbt to {version}'
    )


def sanity_check():
    if sys.version_info[:len(HOMEBREW_PYTHON)] != HOMEBREW_PYTHON:
        python_version_str = '.'.join(str(i) for i in HOMEBREW_PYTHON)
        print(f'This script must be run with python {python_version_str}')
        sys.exit(1)

    # avoid "what's a bdist_wheel" errors
    try:
        import wheel  # noqa
    except ImportError:
        print(
            'The wheel package is required to build. Please run:\n'
            'pip install -r dev_requirements'
        )
        sys.exit(1)


def upgrade_to(args: Arguments):
    set_version(args.path, args.version, args.part)
    dist_path = build_pypi_packages(args.path)
    upload_pypi_packages(dist_path)
    input(
        f'Ensure https://test.pypi.org/project/dbt/{args.version}/ exists '
        'and looks reasonable'
    )
    upload_pypi_packages(dist_path, test=False)
    build_homebrew_package(
        dbt_path=args.path,
        version=args.version,
        homebrew_path=args.homebrew_path,
        set_default=args.homebrew_set_default,
    )


def main():
    sanity_check()
    args = Arguments.parse()
    upgrade_to(args)


if __name__ == '__main__':
    main()
