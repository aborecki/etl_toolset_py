from pyetltools.core.cmd import Cmd


def get_table_ddl(db_conn, table_name, output_file):
    # To install required module:  Install-Module -Name SqlServer -Scope CurrentUser

    ps_script=f"""$db =  Get-SqlDatabase -ServerInstance {db_conn.host} -Name {db_conn.data_source}
$tabs = $db.Tables | Where Name -eq "{table_name}"
$tabs.Script() | Out-File -FilePath "{output_file}"
"""

    import tempfile

    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.ps1')
    tmp.close()

    with open(tmp.name, 'w') as f:
        f.write(ps_script)

    return Cmd("powershell").run(tmp.name)

