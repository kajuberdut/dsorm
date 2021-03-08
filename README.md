<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Thanks again! Now go create something AMAZING! :D
***
***
***
*** To avoid retyping too much info. Do a search and replace for the following:
*** kajuberdut, dsORM, twitter_handle, patrick.shechet@gmail.com, Darned Simple ORM, A single file ORM for SQLite in Python
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

  <h3 align="center">Darned Simple ORM</h3>

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

Darned Simple ORM (dsORM) is a little bit different from other ORMs.
The typical ORM approach is to have developers "map" their classes to a database and then do "magic" to make that database work.
dsORM aims instead to map database components into easy to use Python objects which can be leveraged by your classes for database interactions.

If SQLAlchemy's expression language comes to mind, yes, this is a bit like that. But dsORM is simple, the entire functional code is in a single file which is currently under 1,000 lines.
For comparison, PeeWee, a fairly small ORM is 7,723 lines long in it's main file and that doesn't contain all of it's functional code.
SQLAlchemy as of this writing contains 343,975 lines of Python code (though admittedly it dwarfs dsORM's feature set.)


### Designed for easy integration / modification

* 100% Python 
* No external dependencies
* 100% test coverage
* Functional code in a single file

### Should I use this?
#### You should **not** use dsORM if:
* You need a fully featured and robust ORM supporting multiple back ends
* You don't have any idea how SQL works and need maximal hand holding
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
from dsorm import ID_COLUMN, Column, Database, Table, Where

person = Table(
    table_name="person",
    column=[
        Column.id(),  # This is shorthand for Column("id", int, pkey=True)
        Column(column_name="first_name", nullable=False),
        Column(column_name="last_name", nullable=False),
    ],
)

print(person.sql())


person2 = Table.from_dict(
    "person",
    {
        "id": ID_COLUMN,
        "first_name": {"nullable": False},
        "last_name": {"nullable": False},
    },
)

print(person2.sql())

# See Database example for more details about the Database object
Database(db_path=":memory:", is_default=True).init_db()  # This creates all tables


# Tables have insert, select, and delete methods.
# These return a Statement
stmt = person.insert(
    data=[
        {"first_name": "Jane", "last_name": "Doe"},
        {"first_name": "John", "last_name": "Doe"},
    ],
)

# Statements can be examined with .sql method
print(stmt.sql())

# INSERT INTO Main.person (first_name, last_name)
# VALUES ('Jane', 'Doe'), ('John', 'Doe')

# or executed with .execute()
stmt.execute()

# select returns a list of dicts of rows matching the where
does = person.select(
    where={"first_name": Where.like(target="J%n%")},
    column=[
        "id",
        "first_name || ' ' || last_name AS full_name",  # Note that the columns can be sql
    ],
).execute()

print(does)
# [{"id": 1, "full_name": "John Doe"}, {"id": 2, "full_name": "Jane Doe"}]

# And Delete
person.delete(where={"id": does[0]["id"]}).execute()
print([r["id"] for r in person.select().execute()])
# [2]
```

It's darned simple.

### Further Examples
* [A Practical Example](https://github.com/kajuberdut/dsorm/blob/main/examples/PracticalExample.py)
* [Custom Type Handling & Column Defaults](https://github.com/kajuberdut/dsorm/blob/main/examples/CustomTypeHandlerAndDefault.py)
* [Advanced WHERE clauses](https://github.com/kajuberdut/dsorm/blob/main/examples/AdvancedWhere.py)
* [Configuration](https://github.com/kajuberdut/dsorm/blob/main/examples/AdvancedConfiguration.py)
<!-- * [Statements](https://github.com/kajuberdut/dsorm/blob/main/examples/Statements.py) -->


<!-- ROADMAP -->
## Roadmap

Needed features:
* Easier and more robust JOIN support
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