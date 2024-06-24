# Magic commands
- If you are writing a python code and wanted to execute a sql code, you can perform below code and perform <br>
 `%sql`<br>
 `select xxxxxxxx`
- It can be applicable for python, sql, r, md.

# Creation of dataframes from static data
- dataframes is combination of 2 things, data and column
- You store data in the form of tuples inside the list
  
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/f3025a98-b75f-4cea-bba0-55e7ce625da8)

- It also infers the data

  ## Select in pyspark
  ` df_select = df.select('name','city').show()`
