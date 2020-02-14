crs=["CRQ000001407288",
     "CRQ000001407290",
     "CRQ000001407334",
     "CRQ000001407337"]

url=r"https://remedy.intra.bec.dk/arsys/servlet/ViewFormServlet?form=CHG%3AInfrastructure+Change&server=remedy-itsm&qual=%271000000182%27%3D%22{cr}%22"

print("\n".join([cr+" "+url.replace("{cr}", cr) for cr in crs]))



CreateTable_query = "Create Table my table(a string, b string, c double)"
_.sql(CreateTable_query)