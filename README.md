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
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Darned Simple ORM</h3>

  <p align="center">
    A single file ORM for SQLite in Python
    <br />
    <!-- <a href="https://github.com/kajuberdut/dsorm"><strong>Explore the docs »</strong></a> -->
    <br />
    <br />
    <!-- <a href="https://github.com/kajuberdut/dsorm">View Demo</a> -->
    <!-- · -->
    <a href="https://github.com/kajuberdut/dsorm/issues">Report Bug</a>
    ·
    <a href="https://github.com/kajuberdut/dsorm/issues">Request Feature</a>
  </p>
</p>



<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary><h2 style="display: inline-block">Table of Contents</h2></summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

Darned Simple ORM (dsORM) is designed to be a minimal (single file,) approach to mapping SQL tables to Python Objects.

It provides a management class for the database(s) connections / cursors and tables (for creation, select, insert/update or delete.)


### Built With

* 100% Python with no external dependencies

### Who should use this?
#### You should **not** use dsORM if
* You need a fully featured and robust ORM supporting multiple back ends
* You don't have any idea how SQL works and need maximal hand holding
* You want something that enforces best practices

#### You might want to look at dsORM if
* You know SQL enough to get around and want to avoid some boilerplate
* You are prototyping and want something minimal to stand in for another ORM
* You want to make your own project tailored ORM and can use dsORM as a starting point
* You cannot pip install in your environment and need a single file solution that can be bundled

<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.

### Installing with pip

  ```sh
  pip install git+https://github.com/kajuberdut/dsorm.git
  ```

### Cloning / Developement setup

1. Clone the repo
   ```sh
   git clone https://github.com/kajuberdut/dsorm.git
   ```
2. Pipenv install dev requirements
   ```sh
   pipenv install --dev
   pipenv install -e .
   ```
   For more about pipenv see: [Pipenv Github](https://github.com/pypa/pipenv)



<!-- USAGE EXAMPLES -->
## Usage

### Database
Although more advanced usage may use multiple databases, a single default db can be set globally.

```python
from dsorm import Database, Cursor

Database.default_db = ":memory:"

# Cursor is a context manager, always used in a "with" statement.
# Cursor takes a db_path or uses the default_db set above.
with Cursor() as cur:
    print(cur.execute("SELECT 1 AS thingy"))
    # [{'thingy': 1}]
```

The above shows a few conveniences over using the built in SQLite3 module directly. 
* First, the context manager makes opening/closing the cursor very easy. 
* Second, dsORM automatically employs a dictionary row factory vs. sqlite3's arcane row type.

Conveniences does not an ORM make. Here is a longer example showing dsORM objects.

```python
from dsorm import *  # clean for readme, you'll want to import objects by name.

# The pre_connect wrapper let's you set a function that will be called before the first connection
@pre_connect()
def db_setup(db):
    db.default_db = ":memory:"


# The post_connect wrapper is called once after the first connection is made
@post_connect()
def build(db):
    # This will set our pragam and create our tables.
    # We'll create these bellow and they will be instantiated at the first connection
    db.init_db()


# Let's setup foreign key enforcement which is off by default
conf = Pragma(
    pragma={
        "foreign_keys": 1,
        "temp_store": 2,
    }
)

Person = Table(
    name="person",
    column=[
        Column("id", sqltype="INTEGER", pkey=True),
        Column("first_name", nullable=False),
        Column("last_name", nullable=False),
        Column("screen_name", unique=True),
    ],
)

Email = Table(
    name="email",
    column=[
        Column("id", sqltype="INTEGER", pkey=True),
        Column("email", sqltype="TEXT", nullable=False),
        Column("person_id", nullable=False),
        Person.fkey(on_column="person_id"),
    ],
)

if __name__ == "__main__":
    # Table objects have select, insert, and delete methods
    # Each returns sql and values you can pass to execute
    sql, values = Person.insert(data={"first_name": "John", "last_name": "Doe"})
    with Cursor() as cur:
        cur.execute(sql, values)
        print(cur.execute(*Person.select()))
        # {'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'screen_name': None}

    # Even more convenient:
    # The db object can access any table and run the whole thing for you.
    db = Database()

    # Create inserts a record
    db.create(table="person", data={"first_name": "John", "last_name": "Doe"})

    # Query selects back a list of dicts matching the where clause
    johns = db.query(
        "person",
        where={"first_name": "John"},
        columns=[
            "id",
            "first_name || ' ' || last_name AS full_name",
        ],  # Note that the columns can be freehand sql
    )
    print(johns)
    # [{'id': 1, 'full_name': 'John Doe'}, {'id': 2, 'full_name': 'John Doe'}]

    db.delete("person", where={"id": johns[0]["id"]})
    print(db.query("person"))
    # [{'id': 2, 'first_name': 'John', 'last_name': 'Doe', 'screen_name': None}]

```

It's darned simple.


<!-- ROADMAP -->
## Roadmap

Needed features:
* JOIN between objects
* More WHERE operators (not, or, clause grouping)
* Grouping/Aggregates
* Order/Limit/Offset

See the [open issues](https://github.com/kajuberdut/dsorm/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Add tests, we aim for 100% test coverage
4. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
5. Push to the Branch (`git push origin feature/AmazingFeature`)
6. Open a Pull Request



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