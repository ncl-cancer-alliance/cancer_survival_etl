# utils Folder

By default this folder should just contain an additional __init__.py file similar to the src parent folder.

This folder is used for additional scripts containing functions and code you want to import into your main.py file but want to keep seperated.

One use case of why you might want this is for more general use functions. For example if you have a series of functions that interacts with the SQL database, you can seperate them out into a new file (say "db_util.py") in the utils directory then import this file into main with:

```{python}
import db_util as db
```

Then you can use the functions in db_util.py inside of main.py using the prefix db. 

e.g.
```{python}
db.upload_data(my_database, my_data)
```

The additional benefit of this approach is that it is easier to export this code to other projects and maintain it if it is serperated out from the main.py script.