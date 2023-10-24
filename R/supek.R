library(ssh)


# params
host = "jarneric@login-cpu.hpc.srce.hr"
key_path = "C:/Users/Mislav/.ssh/id_rsa_srce"
paraphrase = "Joskosrce"

session <- ssh_connect(host, keyfile = key_path)
print(session)

ssh_key_info()
# execute command
out <- ssh_exec_wait(session, command = 'whoami')
out <- ssh_exec_wait(session, command = 'sudo apt install r-base -S Joskosrce')
out <- ssh_exec_wait(session, command = 'apptainer pull docker://r-base')
print(out)



library(data.table)
url = "https://snpmarketdata.blob.core.windows.net/qc1000/"
dates = seq.Date(as.Date("1998-01-01"), Sys.Date(), 1)
url_files = paste0(url, dates, ".csv")
qc1000 = lapply(url_files, function(x) tryCatch(fread(x), error = function(e) NULL))
