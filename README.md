# Local Setup

## Repository

```zsh
git clone https://cbsp-abnamro@dev.azure.com/cbsp-abnamro/GRD0001018/_git/betl-src-poc
```

Now you can open this folder in your prefered IDE (VSC / PyCharm)


## Virtual environment

Make sure you create an virtual environment

```bash
python -m venv .venv
```

And activate the environment

**MacOS**
```bash
source .venv/bin/activate
```

**Windows**
- Powershell:

    ```bash
    .venv\Scripts\activate.bat
    ```
- CMD:

    ```bash
    .venv\Scripts\Activate.ps1
    ```

### Nexus access
In case you did not configure your pip configuration yet, you have to specify the `--index-url` for 
installing packages on ABN network with your local machine. Please follow [these instructions](https://confluence.int.abnamro.com/display/GRIDAD/Configuring+PIP+for+Nexus3+Access).

Now you are ready to install all (development) requirements.

```zsh
pip install -r requirements.txt"
pip install -r requirements_test.txt"
```

### Formatting & Linting
All linting and formatting rules are configured in `pyproject.toml` file.

Check the [documentation](https://docs.astral.sh/ruff/rules/) of `ruff` for detailed explanation of all these rules


**Note**
Before committing code to the repository, make sure you install `pre-commit`

```zsh
pip install -r requirements_pre-commit.txt"
pre-commit install
```

You can validate installation by running `pre-commit` on all files

```zsh
pre-commit run --all-files
```

Everytime when you commit your code, the `pre-commit` will be executed on
the committed.

You can also directly format/lint your code according to `ruff` configuration in `pyproject.toml` file

```zsh
ruff format .
ruff --check . --fix
```

**Note**
Install Ruff Extension in Visual Code Studio to get direct feedback, while developing.

### Testing
Since you have installed the development dependencies, you can run the tests from your terminal

```zsh
pytest
```

Or use the **Testing** Extension in your IDE.

Since Spark is a dependency in this project it is required to have it locally for the tests

#### MacOS
Use SDK man for installing JAVA on your MacBook. First run:

```zsh
curl -s "https://get.sdkman.io" | bash
```

then install an appropriate version of JDK (in this example the version 17 from Oracle)

```zsh
sdk install java 17.0.0-oracle
```

To verify the installation, please run

```zsh
java --version
```

should give something like:

```
java 17 2021-09-14 LTS
Java(TM) SE Runtime Environment (build 17+35-LTS-2724)
Java HotSpot(TM) 64-Bit Server VM (build 17+35-LTS-2724, mixed mode, sharing)
```

For the rest, you don't need to install spark (just `pip install pyspark` as part of the requirements of this project).

#### Windows
For Windows, JAVA should be installed and configured in your environment variables. 
Follow the instructions from the [ABN documentation](https://confluence.int.abnamro.com/plugins/servlet/samlsso?redirectTo=%2Fpages%2Fviewpage.action%3FspaceKey%3DGRIDAD%26title%3DSetup%2Bpyspark%2Bon%2Ba%2Blocal%2Benvironment). 

## Extensions for Visual Code Studio

- Ruff: Formatting & Linting checks
- autoDocstring - Python Docstring Generator
- Todo Tree: To Check all TODO comments in your code
- Even Better TOML: TOML Syntax highlighter
- Rainbow CSV: Highlight CSV files for readability
- GitLens: Interactive inline code history
