$script:version = "1.0.3.130411"
Write-Verbose $version

#/space/avamar/var/mc/server_data/postgres/data/pg_hba.conf
#chmod u+w pg_hba.conf
#host all all 10.0.0.0/8 trust
#dpnctl stop mcs;dpnctl start mcs

(gi .\Mono.Security.dll) | unblock-file
(gi .\npgsql.dll) | unblock-file

[void][system.reflection.Assembly]::LoadFrom((gi .\npgsql.dll).fullname)


#Connect-vPostgres -server 10.241.67.243 -username viewuser -password viewuser1 -database mcdb -port 5555
#Get-vPostgresDataSet -server 10.241.67.234 -username postgres -password P@ssword1! -query "CREATE DATABASE vcp_history WITH ENCODING='UTF8' OWNER=postgres;"
#Get-vPostgresDataSet -server 10.241.67.234 -username postgres -password P@ssword1! -query "CREATE DATABASE vcp_history WITH ENCODING='UTF8' OWNER=postgres;"
#Get-vPostgresDataSet -server 10.241.67.243 -query "select * from public.v_dpnsummary;"
#Get-vPostgresDataSet -server 10.241.67.243 -query "select * from public.v_dpnsummary;"
function Connect-vPostgres {
    [CmdletBinding()]
    param(
        $server = $(Read-Host "SQL Server Name"),
        $username,
        $password,
        $database,
        $port
    )
    Process {
        if(!$global:vPostgresConnection) { $global:vPostgresConnection = @{} }
        $global:vPostgresConnection.Remove($Server)
        $global:vPostgresConnection.$server = New-Object Npgsql.NpgsqlConnection
        $global:vPostgresConnection.$server.ConnectionString = "server=$server;port=$port;user id=$username;password=$password;database=$database;pooling=false"
        $result = Get-vPostgresDataSet -server $server -query "Select 1=2"
        if(!$result) { Throw "Problem connecting to database" }
        
    }
}

function Get-vPostgresDataSet {
    param 	( $server,
    		  $username,
    		  $password,
    		  $database,
    		  $query,
    		  $port,
              $sqlparam,
              $connection,
              $timeout )

    function Get-SqlDataTable {
        [CmdletBinding()]
        Param(
            $server,
            $connection,
            $Query, 
            $sqlparam, 
            [switch]$close,
            [int]$timeout
        )
        Process {
            if($server) { 
                $tmpvPostgres = $global:vPostgresConnection.$server
            }elseif($connection){
                $tmpvPostgres = $connection
            }
    	    if (-not ($tmpvPostgres.State -like "Open")) { $tmpvPostgres.Open() }
    	    $SqlCmd = New-Object npgsql.npgsqlCommand $Query, $tmpvPostgres
            if($timeout) { $SqlCmd.CommandTimeout = $timeout}
            if($sqlparam) { $sqlparam.psobject.properties | %{ [void]$sqlCmd.Parameters.AddWithValue($_.name,$_.value) } }
            $SqlAdapter = New-Object npgsql.npgsqlDataAdapter
    	    $SqlAdapter.SelectCommand = $SqlCmd
    	    $DataSet = New-Object System.Data.DataSet
    	    $SqlAdapter.Fill($DataSet) | Out-Null
    	    if($close) { $tmpvPostgres.Close() }
    	    return $DataSet.Tables[0]
        }
    }

    if(!$connection -and !$global:vPostgresConnection.$server -and ($server -and $username -and $password)) { 
        Connect-vPostgres $server $username $password $database $port 
    }

    try {
        if($connection) {
            Get-SqlDataTable -connection $connection -query "Select 1=2" | Out-Null
        }else {
            Get-SqlDataTable -server $server -query "Select 1=2" | Out-Null
        }
    } catch {
        Write-Verbose "Sensed problem with DB connection, trying to close and open DB connection"
        if(!$connection) { 
            $connection = $global:vPostgresConnection.$server
        }
        $connection.close()
        $connection.open()
    }

    Write-Verbose "Query: $query"
    Write-Verbose "SqlParam: $sqlParam"

    Get-SqlDataTable -connection $connection -server $server -query $Query -sqlparam $sqlparam -timeout $timeout | Select * -ExcludeProperty RowError,RowState,Table,ItemArray,HasErrors
}



 #need to
 #Copy-PostgresDataset -srcConnection $vPostgresConnection."10.241.67.243" -dstConnection $vPostgresConnection."10.241.67.234" -srcCommand "COPY (SELECT * FROM public.v_activities) TO STDOUT" -dstCommand "COPY v_activities FROM STDIN"
 function Copy-PostgresDataset {
    [CmdletBinding()]
    param(
        $srcConnection,
        $dstConnection,
        $srcCommand,
        $dstCommand
    )
    Process {
        if($srcConnection.State.ToString() -ne "Open") { $srcConnection.open() }
        if($dstConnection.State.ToString() -ne "Open") { $dstConnection.open() }
        $command_cout = new-object Npgsql.NpgsqlCommand($srcCommand,$srcConnection)
        $cout = new-object Npgsql.NpgsqlCopyOut($command_cout,$srcConnection)
        $command_cin = new-object Npgsql.NpgsqlCommand($dstCommand,$dstConnection)
        $cin = new-object Npgsql.NpgsqlCopyIn($command_cin,$dstConnection)

        #$outStream_cout = [console]::OpenStandardOutput()
        #$serverEncoding_cout = [System.Text.Encoding]::BigEndianUnicode
        #$outEncoding_cout = [System.Text.Encoding]::ASCII

        try {
            $cout.start()
            $cin.start()
            $copyOutStream = $cout.CopyStream
            $copyInStream = $cin.CopyStream
            #[byte[]] $buf_cout = $cout.Read
            [byte[]]$buf_cout = New-Object byte[] 9
            #[Console]::Out.Write($buf_cout,0,$buf_cout.length)
    
            while (([int]$i=$copyOutStream.Read($buf_cout,0,$buf_cout.length)) -gt 0) {
                $copyInStream.Write($buf_cout,0,$i)
                #[System.Text.Encoding]::Convert($serverEncoding,$outEncoding,$buf,0,$i)
                #[Console]::Out.Write($buf_cout,0,$i)
            } 
            $copyInStream.close() | Out-Null
            $copyOutStream.close() | Out-Null
        } catch {
            try {
                $cin.end() | Out-Null
                $cout.end() | Out-Null
            }catch {
                throw $_
            }
        }
    }

}

#Get-vPostgresDataSet -connection $DefaultVCPDatabase -query "DELETE from vcloudurn where (vcloudobject->'Id') LIKE '%vm%'AND not (vcloudobject?'vminstanceuuid')"

#Get-PostgresHstore -server 10.241.67.234 -query "select * from vcloudurn where (vcloudobject->'Name')='TESTORG2'"
#Get-PostgresHstore -server 10.241.67.234 -table vcloudurn -hstorecolumn vcloudobject -hashEq @{Name="TESTORG2"}
Function Get-PostgresHstore {
    [CmdletBinding()]
    param($server,$query,[array]$column,$hstorecolumn,$hstorekey,$hashEq,$table)
    Process {
        if($hstorecolumn -and $table) {
            [string]$columns = ((@($hstorecolumn)+[array]$column) | where {$_}) -join ","
            $query = "select $($columns) from $($table)"
        }elseif(!$query) {
            Write-Host "Need to specify -hstorecolumn and -table or -query"
        }

        if($hstorekey -and $hstorecolumn) {
            $query = "$($query) where $hstorecolumn ? '$($hstorekey)'"
        }
        if($hashEq) {
            if($hashEq) { $strWhere = " where ("+(($hashEq.keys | %{ 
                "$($hstorecolumn)->`'$($_)`'=`'$($hashEq.$_)`'" 
            }) -join " AND ")+")" }
            $query = "$($query) $strWhere"
        }

        Get-vPostgresDataSet -server $server  -query $query | %{
            $hashResults = @{}
            $_.Psobject.properties | %{
                if($_.value -match "=>") {
                    $_.value -split ", " | %{ 
                            ($param,$value) = $_ -split "=>"
                            $param = $param -replace "^`"|`"$"
                            $value = $value -replace "^`"|`"$"
                            $hashResults.$Param = $Value 
                    }
                }else {
                    $hashResults.($_.name) = $_.value
                }
            }
            New-Object -type psobject -property $hashResults
        }
    }
}

Function Disconnect-vPostgres {
    [CmdletBinding()]
    param($server)
    Process {
        $global:vPostgresConnection.$server.Close()
        $global:vPostgresConnection.$server.Dispose()
        $global:vPostgresConnection.Remove($server)
    }
}
