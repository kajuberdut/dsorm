<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Thanks again! Now go create something AMAZING! :D
***
***
***
*** To avoid retyping too much info. Do a search and replace for the following:
*** kajuberdut, dsORM, twitter_handle, patrick.shechet@gmail.com, Data Structure ORM, A single file ORM for SQLite in Python
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/kajuberdut/dsorm">
    <img src="https://github.com/kajuberdut/dsorm/blob/main/images/logo.png?raw=true" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Data Structure ORM</h3>

  <p align="center">
    A single file ORM for SQLite in Python
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary><h2 style="display: inline-block">Table of Contents</h2></summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
    </li>
    <li><a href="#usage">Usage</a>
      <ul>
        <li><a href="#further-examples">Further Examples</a></li>
      </ul>
    </li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

Data Structure ORM (dsORM) a tiny, extensible ORM that leverages Python's built in data structures.
dsORM easily converts dictionaries, enums, and dataclasses into tables.
For fine control you can craft tables, columns, and constraints from provided base classes.

If SQLAlchemy's expression language comes to mind, yes, this is a bit like that. But dsORM is much simpler. The entire functional code is in a single file which is currently under 1,500 lines.
For comparison, PeeWee, a fairly small ORM is 7,723 lines long in it's main file and that doesn't contain all functional code.
SQLAlchemy as of this writing contains 343,975 lines of Python code (admittedly, it dwarfs dsORM's feature set.)


### Designed for easy integration / modification

* 100% Python 
* No external dependencies
* 100% test coverage
* Functional code in a single file

### Should I use this?
#### You should **not** use dsORM if:
* You can't work with SQLite
* You want something that enforces best practices

#### You should use dsORM if:
* You know SQL enough to get around and want to avoid some boilerplate
* You are prototyping and want something minimal to stand in for another ORM
* You want to make your own project tailored ORM and can use dsORM as a starting point
* You cannot pip install in your environment and need a single file solution that can be bundled

<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.

### Installing with pip

  ```sh
  pip install dsorm
  ```

For information about cloning and dev setup see: [Contributing](#Contributing)


<!-- USAGE EXAMPLES -->
## Usage
Here is an example showing basic usage.

```python
import dataclasses
from enum import Enum

from dsorm import Comparison, Database, DataClassTable, make_table

# the .memory() constructor is equivilent to Database(db_path=":memory:", is_default=True)
db = Database.memory()


# Leverage enums for efficient small lookup tables
@make_table
class Team(Enum):
    UNASSIGNED = 0
    RED = 1
    BLUE = 2


@make_table
@dataclasses.dataclass
class Person(DataClassTable):
    first_name: str = None
    last_name: str = None
    team: Team = Team.UNASSIGNED


person = db.table("Person")

print(person.sql())
# CREATE TABLE IF NOT EXISTS person (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT);


# Tables have insert, select, and delete methods which return subclasses of dsorm.Statement
stmt = person.insert(
    data=[
        {"first_name": "John", "last_name": "Doe", "team": Team.BLUE},
    ],
)

# Statements can be examined with .sql method
print(stmt.sql())
# INSERT INTO [Person] (first_name, last_name, team) VALUES ('John', 'Doe', 2)

# or executed with .execute()
stmt.execute()

# Subclasses of DataClassTable inherit a save method
Jane = Person(first_name="Jane", last_name="Doe", team=Team.RED).save()

# Select returns a list of dicts of rows matching the where
doe_family = person.select(
    where={"first_name": Comparison.like(target="J%n%")},
).execute()

print(doe_family)
# [
#     {'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'team': <Team.BLUE: 2>
#     },
#     {'id': 2, 'first_name': 'Jane', 'last_name': 'Doe', 'team': <Team.RED: 1>
#     }
# ]

# And Delete
person.delete(where={"id": doe_family[0]["id"]}).execute()
print(person.select(column=["id", "first_name"]).execute())
# [{'id': '2', 'first_name': 'Jane'}]

```

[The same example without comments or prints. Shows efficiency and readability of DDL/DML](https://github.com/kajuberdut/dsorm/blob/main/examples/ReadmeExample_NoComments.py)


### Further Examples
* [A Practical Example](https://github.com/kajuberdut/dsorm/blob/main/examples/PracticalExample.py)
* [Compound WHERE clauses and Tables from Enum](https://github.com/kajuberdut/dsorm/blob/main/examples/AdvancedWhere.py)
* [Joins and Database from Dict](https://github.com/kajuberdut/dsorm/blob/main/examples/JoinExample.py)
* [Custom Type Handling & Column Defaults](https://github.com/kajuberdut/dsorm/blob/main/examples/CustomTypeHandlerAndDefault.py)
* [Store Python Objects with Pickle Data Handler](https://github.com/kajuberdut/dsorm/blob/main/examples/PickleData.py)
* [Configuration](https://github.com/kajuberdut/dsorm/blob/main/examples/AdvancedConfiguration.py)
<!-- * [Statements](https://github.com/kajuberdut/dsorm/blob/main/examples/Statements.py) -->


<!-- ROADMAP -->
## Roadmap

Needed features:
* Subquery/CTE support
* Grouping/Aggregates
* Order/Limit/Offset

See the [open issues](https://github.com/kajuberdut/dsorm/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Add tests, we aim for 100% test coverage [Using Coverage](https://coverage.readthedocs.io/en/coverage-5.3.1/#using-coverage-py)
4. execute: py.test --cov-report xml:cov.xml --cov
5. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
6. Push to the Branch (`git push origin feature/AmazingFeature`)
7. Open a Pull Request

### Cloning / Development setup
1. Clone the repo and install
    ```sh
    git clone https://github.com/kajuberdut/dsorm.git
    cd dsorm
    pipenv install --dev
    ```
2. Run tests
    ```sh
    pipenv shell
    py.test
    ```
  For more about pipenv see: [Pipenv Github](https://github.com/pypa/pipenv)



<!-- LICENSE -->
## License

Distributed under the BSD Two-clause License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

Patrick Shechet - patrick.shechet@gmail.com

Project Link: [https://github.com/kajuberdut/dsorm](https://github.com/kajuberdut/dsorm)




<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/kajuberdut/dsorm.svg?style=for-the-badge
[contributors-url]: https://github.com/kajuberdut/dsorm/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/kajuberdut/dsorm.svg?style=for-the-badge
[forks-url]: https://github.com/kajuberdut/dsorm/network/members
[stars-shield]: https://img.shields.io/github/stars/kajuberdut/dsorm.svg?style=for-the-badge
[stars-url]: https://github.com/kajuberdut/dsorm/stargazers
[issues-shield]: https://img.shields.io/github/issues/kajuberdut/dsorm.svg?style=for-the-badge
[issues-url]: https://github.com/kajuberdut/dsorm/issues
[license-shield]: https://img.shields.io/badge/License-BSD%202--Clause-orange.svg?style=for-the-badge
[license-url]: https://github.com/kajuberdut/dsorm/blob/main/LICENSE
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/patrick-shechet