## CB reporting: Circulo de Credito
## Reporting schedule: bi-weekly on the 22nd and 7th day of the following month for payments up to 15th and 31st

## Legally Required fields: 
# 1.ClaveActualOtorgante: "0025160028"
# 2.NombreOtorgante: "Tala Mobile"
# 3.FechaExtraccion: "YYYYDDMM" date when report is issued
# 4.Version: 4
# 5.ApellidoPaterno: loans.people 
# 6.ApellidoMaterno: loans.people_additional_data
# 7.Nombres: (loans) people.name 
# 8.FechaNacimiento: (loans) people.dob
# 9.RFC: (loans) people.nid Note: first 10 digist of CURP which is the national ID in other countries
# 10.Direccion: loans.people_additional_data
# 11.ColoniaPoblacion: loans.people_additional_data
# 12.DelegacionMunicipio: loans.people_additional_data
# 13.Ciudad: loans.people_additional_data
# 14.Estado: loans.people_additional_data
# 15.CP: loans.people_additional_data
# 16.CuentaActual: (credit)loan_basic.loan_id
# 17.TipoResponsabilidad: I
# 18.TipoCuenta:F
# 19.TipoContrato: PP
# 20.ClaveUnidadMonetaria: MX
# 21.NumeroPagos: (loans) loan_type.repayment_counts OR (credit) loan_basic.installment_count
# 22.FrecuenciaPagos: (loans) loan_type.repayment_frequency OR (credit) loan_basic.installment_period
# 23.MontoPagar: NA TO CALCULATE using (credit) loan_basic.amount AND (credit) loan_basic.interest_amount AND (credit) loan_basic.installment_count
# 24.FechaAperturaCuenta: (loans) loan_funding.timestamp or (credit) loan_basic.disbursement_time
# 25.FechaUltimoPago: (credit) loan_basic.last_payment_date Note:"19010101" if borrower has NOT made pmts
# 26.FechaCorte: (loans) loan_repayment.date_time OR (credit) loan_basic.paid_off_date
# 27.CreditoMaximo: (loans) loan.amount OR (credit) loan_basic.amount
# 28.SaldoActual: (loans) loan.still_owed OR (credit) loan_basic.still_owed
# 29.LimiteCredito: (loans) people_credit_limit.credit_limit OR (credit) people_basic.max_credit_limit_offered
# 30.SaldoVencido: (credit) collection.amount_sent_to_collection Note: (must be <= SaldoActual)
# 31.PagoActual: " V" if current, if non-current then use 01-84 to report late payment periods
# 32.SaldoInsoluto: (credit) loan_basic.still_owed - (credit) loan_basic.interest_amount Note: only include principal

source('helper.R')
library(RJSONIO)
library(dplyr)

## sample time period. NOTE: tend = "fecha corte" needs to be either the 15th or the last day of the month. Adjust as needed.
tstart <- '2017-11-10 00:00:00'
tend <- Sys.Date()
#tend <- Sys.Date()-7

# Pull loan and payment behavior data from Yuting's credit table based on desired time period. Note: TAKE OUT LIMIT 100
ssh_looker()
loan_data <- get_db('credit', paste("SELECT l.person_id, l.loan_application_id, l.status, l.amount, l.interest_rate, l.disbursement_time, 
                                    l.installment_count, l.installment_period, l.interest_assessed, l. due_date, l.last_payment_date, l.paid_off_date,
                                    l.still_owed, p.id_number, p.max_credit_limit_offered
                                     FROM loan_basic_mx l JOIN people_basic_mx p
                                     ON l.person_id = p.person_id
                                     WHERE disbursement_time >=", shQuote(tstart), "AND disbursement_time <=", shQuote(tend)))
kill_looker()

setnames(loan_data,"loan_application_id","CuentaActual")
loan_data[, FrecuenciaPagos := ifelse(installment_period == 30,"M",
                                                   ifelse(installment_period == 7,"S","Q"))]
loan_data[, MontoPagar := round((amount+interest_assessed)/installment_count,0)]
loan_data[, FechaAperturaCuenta := format(as.Date(disbursement_time), "%Y%m%d")]
loan_data[, FechaUltimoPago := ifelse(!is.na(last_payment_date),format(as.Date(last_payment_date),"%Y%m%d"),"19010101")]
loan_data[, FechaCorte := format(as.Date(tend), "%Y%m%d")]

setnames(loan_data,"amount","CreditoMaximo")
setnames(loan_data,"still_owed","SaldoActual")
setnames(loan_data,"max_credit_limit_offered","LimiteCredito")

# For item #31 if current, it's " V", if late from 22nd day after due date, then put # past due 30 day period
loan_data[, PagoActual := ifelse(as.Date(due_date)+21>tend && SaldoActual == 0,"V",
                                 ifelse(as.Date(due_date)+51>tend && SaldoActual != 0,"01",
                                        ifelse(as.Date(due_date)+81>tend && SaldoActual != 0,"02",
                                               ifelse(as.Date(due_date)+111>tend && SaldoActual != 0,"03",
                                                      ifelse(as.Date(due_date)+141>tend && SaldoActual != 0,"04"),"05"))))]

# For item #30.SaldoVencido and #32 SaldoInsoluto, we'll need it to be consistent with previous status column
loan_data[, SaldoVencido := ifelse(PagoActual !="V",SaldoActual,0)]
loan_data[, SaldoInsoluto := ifelse(PagoActual !="V" && SaldoActual>0,SaldoActual - interest_assessed,0)]

## pull name & address info from people and people additional table, fields 5 to 15 above
ssh_replica_mx()
people_data <- get_db_mx('loans', paste("SELECT id, last_name, first_name, middle_name, dob, nid, country, data 
                                        FROM people JOIN people_additional_data 
                                        ON people.id = people_additional_data.person_id
                                        WHERE id IN (",paste(loan_data$person_id, collapse = ','),")"))
kill_replica_mx()

# to remove those NA rows only if needed
#people_data <- people_data[!data == "[]"]

# structure the table
json_filea <- paste0('[', paste(people_data$data[1], collapse = ','), ']')
json_filea <- fromJSON(json_filea)

json_filea <- lapply(json_filea, function(x){
  x[sapply(x, is.null)] <- NA
  unlist(x)
})

a <- data.table(do.call("rbind", json_filea))

# add the rest of the rows
for(i in c(2:nrow(people_data))){
  json_fileb <- paste0('[', paste(people_data$data[i], collapse = ','), ']')
  
  json_fileb <- fromJSON(json_fileb)
  json_fileb <- lapply(json_fileb, function(x) {
    x[sapply(x, is.null)] <- NA
    unlist(x)
  })
  
  b <- data.table(do.call("rbind", json_fileb))
  a <- bind_rows(a,b)
}

# add person_id into the table (can do matching of nid in people table with PERSONAL.nid in new_people_data for MX)
new_people_data <- data.table(a)
new_people_data <- new_people_data[,person_id := people_data$id]
#keep national ID and address info only from new_people_data 
# NOTE: The columns are defined by the first person in the list (a) therefore the data that is actually pulled from "data" col may change as we ask for different fields later on
new_people_data  <- new_people_data[,c(27,c(1:6),c(10:16),c(26))]
new_people_data[ , RFC := substring(PERSONAL.nid,1,10)]
setnames(new_people_data,"person_id","id")
setnames(new_people_data,"PERSONAL.first_name","Nombre")
setnames(new_people_data,"PERSONAL.middle_name","SegundoNombre")
setnames(new_people_data,"PERSONAL.last_name","ApellidoPaterno")
setnames(new_people_data,"PERSONAL.maternal_last_name","ApellidoMaterno")
setnames(new_people_data,"PERSONAL.dob","FechaNacimiento")
setnames(new_people_data,"PERSONAL.nid","CURP")
setnames(new_people_data,"PERSONAL.address_street","Calle")
setnames(new_people_data,"PERSONAL.address_exterior_number","NumeroExterior")
setnames(new_people_data,"PERSONAL.address_interior_number","NumeroInterior")
setnames(new_people_data,"PERSONAL.address_town","ColoniaPoblacion")
setnames(new_people_data,"PERSONAL.address_municipality","DelegacionMunicipio")
setnames(new_people_data,"PERSONAL.address_state","Estado")
setnames(new_people_data,"PERSONAL.address_zipcode","CP")
setnames(new_people_data,"PERSONAL.address_city","Ciudad")

#OR use this method to avoid binding rows as this can create shifts in information across borrowers
#library(splitstackshape)
#new_people_data <- data.frame(people_data)
#new_people_data <- cSplit(new_people_data, splitCols = "data", sep = ",", direction = "wide", drop = TRUE)

# Add items 1 & 4 above (IDs provided by credit bureau) before merging with people data
report <- data.table(people_data$id)
report[, ClaveActualOtorgante := "0025160028"]
report[, NombreOtorgante := "Tala Mobile"]
report[, FechaExtraccion := format(Sys.Date(),"%Y%m%d")]
report[, Version := "4"]
setnames(report,"V1","id")

# Merge specific address info from data column in people table to report
report <- merge(report,new_people_data,  by = "id")

# clean data, use upper case for everything Note: might run into certain issues with characters specific to Spanish like "ñ" for example
library(stringr)
report$Nombre <-toupper(str_trim(report$Nombre))
report$SegundoNombre <-toupper(str_trim(report$SegundoNombre))
report$ApellidoPaterno <-toupper(str_trim(report$ApellidoPaterno))
report$ApellidoMaterno <-toupper(str_trim(report$ApellidoMaterno))
report$Calle <-toupper(str_trim(report$Calle))
report$ColoniaPoblacion <-toupper(str_trim(report$ColoniaPoblacion))
report$DelegacionMunicipio <-toupper(str_trim(report$DelegacionMunicipio))
report$Ciudad <-toupper(str_trim(report$Ciudad))

# change state code from ISO city code to CC specific code
report[, Estado := ifelse(Estado=="CMX","CDMX",
                                 ifelse(Estado =="AGU","AGS",
                                        ifelse(Estado=="BCN","BC",
                                               ifelse(Estado=="CAM","CAMP",
                                                      ifelse(Estado=="COA","COAH",
                                                          ifelse(Estado=="CHP","CHIS",
                                                             ifelse(Estado=="CHH","CHIH",
                                                                    ifelse(Estado=="DUR","DGO",
                                                                           ifelse(Estado=="GUA","GTO",
                                                                                  ifelse(Estado=="HID","HGO",
                                                                                         ifelse(Estado=="MIC","MICH",
                                                                                                ifelse(Estado=="NLE","NL",
                                                                                                       ifelse(Estado=="QUE","QRO",
                                                                                                              ifelse(Estado=="ROO","QROO",
                                                                                                                     ifelse(Estado=="TAM","TAMP",
                                                                                                                            ifelse(Estado=="TLA","TLAX",Estado))))))))))))))))]

# Add more account codes from Circulo de Credito
report[, TipoResponsabilidad := "I"]
report[, TipoCuenta := "F"]
report[, TipoContrato := "PP"]
report[, ClaveUnidadMonetaria := "MX"]

report <- merge(report,loan_data, by.x = "id", by.y = "person_id", all.y = T)

# Adjust final column names and clean data as needed (concatenate names and street with address numbers)
report$Nombres <- ifelse(!is.na(report$SegundoNombre),paste(report$Nombre, report$SegundoNombre, sep = " "),report$Nombre)
report$Nombres <-toupper(str_trim(report$Nombres))

report$Direccion <- ifelse(!is.na(report$NumeroInterior),paste(report$Calle,report$NumeroExterior,report$NumeroInterior,sep = " "),paste(report$Calle, report$NumeroExterior,sep = " "))
report$Direccion <-toupper(str_trim(report$Direccion))

report[, FechaNacimiento := format(as.Date(dob),"%Y%m%d")]
setnames(report,"nid","RFC")

# change order and column names to match Circulo de Credito report
cb_columns <- c("ClaveActualOtorgante",
                        "NombreOtorgante",
                        "FechaExtraccion",
                        "Version",
                        "ApellidoPaterno",
                        "ApellidoMaterno",
                        "Nombres",
                        "FechaNacimiento",
                        "RFC",
                        "Direccion",
                        "ColoniaPoblacion",
                        "DelegacionMunicipio",
                        "Ciudad",
                        "Estado",
                        "CP",
                        "CuentaActual",
                        "TipoResponsabilidad",
                        "TipoCuenta",
                        "TipoContrato",
                        "ClaveUnidadMonetaria",
                        "FrecuenciaPagos",
                        "MontoPagar",
                        "FechaAperturaCuenta",
                        "FechaUltimoPago",
                        "FechaCorte",
                        "CreditoMaximo",
                        "SaldoActual",
                        "LimiteCredito",
                        "PagoActual",
                        "SaldoInsoluto")

final <- subset(report, select=cb_columns)

## Now add "Cifras control" which are 
final[, TotalSaldosActuales := sum(SaldoActual)]
final[, TotalSaldosVencidos := sum(SaldoVencido)]
final[, TotalElementosNombreReportados := nrow(final)]
final[, TotalElementosEmpleoReportados := 0]
final[, TotalElementosCuentaReportados := nrow(final)]
final[, NombreOtorgante := "Tala Mobile"]
final[, DomicilioDevolucion := "amarcos@tala.co"]

write.csv(final, file = paste("0025160028_TalaMobile_",format(tend,"%Y%m%d"),"_PRUEBA1.csv", sep = ""), row.names = FALSE)
