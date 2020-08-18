crs=["CRQ000001416357",
     "CRQ000001416359",
     "CRQ000001416361",
     "CRQ000001416469",
     "CRQ000001416364"]

url=r"https://remedy.intra.bec.dk/arsys/servlet/ViewFormServlet?form=CHG%3AInfrastructure+Change&server=remedy-itsm&qual=%271000000182%27%3D%22{cr}%22"

print("\n".join([cr+" "+url.replace("{cr}", cr) for cr in crs]))


