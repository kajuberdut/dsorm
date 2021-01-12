<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Thanks again! Now go create something AMAZING! :D
***
***
***
*** To avoid retyping too much info. Do a search and replace for the following:
*** kajuberdut, dso, twitter_handle, patrick.shechet@gmail.com, Darned Simple ORM, A single file ORM for SQLite in Python
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
  <a href="https://github.com/kajuberdut/dso">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Darned Simple ORM</h3>

  <p align="center">
    A single file ORM for SQLite in Python
    <br />
    <!-- <a href="https://github.com/kajuberdut/dso"><strong>Explore the docs »</strong></a> -->
    <br />
    <br />
    <!-- <a href="https://github.com/kajuberdut/dso">View Demo</a> -->
    <!-- · -->
    <a href="https://github.com/kajuberdut/dso/issues">Report Bug</a>
    ·
    <a href="https://github.com/kajuberdut/dso/issues">Request Feature</a>
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

Darned Simple ORM (dso) is designed to be a minimal (single file,) approach to mapping SQL tables to Python Objects.

It provides a management class for the database(s) connections / cursors and tables (for creation, select, insert/update or delete.)


### Built With

* 100% Pure Python



<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.

### Installing with pip

This is an example of how to list things you need to use the software and how to install them.
  ```sh
  pip install git+https://github.com/kajuberdut/dso.git
  ```

### Cloning / Developement setup

1. Clone the repo
   ```sh
   git clone https://github.com/kajuberdut/dso.git
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
Although more advanced usage may use multiple databases, it is best to start by declaring a default.

```python
from dso import Database

Database.set_default_db(":memory:")

```
Database is a context manager, always used in a "with" statement.

```python
# With above import and default lines
with Database() as db:
  cursor = db.execute("SELECT 1")
  print(cursor.fetchall())

# [{'thingy': 1}]
```

The above shows a few conveniences over using the built in SQLite3 module directly. 
* First, the context manager makes opening/closing the cursor fairly effortless. 
* Second, dso automatically employs a dictionary row factory instead of some arcane row type.

However, a few conveniences does not an ORM make so let's show off the actual object creation.

```python
from dso import Column, Database, ForeignKey, Pragma, Table, init_db

Database.set_default_db(":memory:")

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
        ForeignKey(column="person_id", reference_table=Person, reference_column="id"),
    ],
)

if __name__ == "__main__":
    # This will set our pragam and create our tables from above
    init_db()

    with Database() as db:

        # Table objects have select, insert (can update), and delete methods that simply return sql you can execute
        sql, values = Person.insert(data={"first_name": "John", "last_name": "Doe"})

        print(sql)
        # INSERT INTO person ( first_name
        #                    , last_name
        #                    )
        # VALUES(:first_name, :last_name);

        db.execute(sql, values)

        print(db.execute(*Person.select()).fetchone())
        # {'id': 1, 'first_name': 'John', 'last_name': 'Doe', 'screen_name': None}

        # Even more convenient, the db object can access any table and run the whole thing for you.
        # Create inserts a record
        db.create(table="person", data={"first_name": "John", "last_name": "Doe"})
        # query selects back a list of records matching the where clause
        johns = db.query(
            "person",
            where={"first_name": "John"},
            columns=[
                "id",
                "first_name || ' ' || last_name AS full_name",
            ],  # Note that the columns can be freehand sql
        )
        print(johns)
        # Finally delete
        db.delete("person", where={"id": johns[0]["id"]})

```

That's pretty much it, its' darned simple.


<!-- ROADMAP -->
## Roadmap

See the [open issues](https://github.com/kajuberdut/dso/issues) for a list of proposed features (and known issues).



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to be learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request



<!-- LICENSE -->
## License

Distributed under the BSD Two-clause License. See `LICENSE` for more information.



<!-- CONTACT -->
## Contact

Patrick Shechet - patrick.shechet@gmail.com

Project Link: [https://github.com/kajuberdut/dso](https://github.com/kajuberdut/dso)




<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/kajuberdut/dso.svg?style=for-the-badge
[contributors-url]: https://github.com/kajuberdut/dso/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/kajuberdut/dso.svg?style=for-the-badge
[forks-url]: https://github.com/kajuberdut/dso/network/members
[stars-shield]: https://img.shields.io/github/stars/kajuberdut/dso.svg?style=for-the-badge
[stars-url]: https://github.com/kajuberdut/dso/stargazers
[issues-shield]: https://img.shields.io/github/issues/kajuberdut/dso.svg?style=for-the-badge
[issues-url]: https://github.com/kajuberdut/dso/issues
[license-shield]: https://img.shields.io/badge/License-BSD%202--Clause-orange.svg?style=for-the-badge
[license-url]: https://github.com/kajuberdut/dso/blob/main/LICENSE
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/patrick-shechet