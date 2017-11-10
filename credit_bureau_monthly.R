## CB reporting: Circulo de Credito
## Reporting schedule: bi-weekly on the 22nd and 7th day of the following month for payments up to 15th and 31st

## Legally Required fields: 
# RFC <- first 10 digist of CURP which is the national ID in other countries
# Primer Nombre: loans.people 
# Segundo y otros Nombres: loans.people_additional_data 
# Apellido Paterno: loans.people 
# Segundo Apellido: loans.people_additional_data
# Fecha de Nacimiento: loans.people
# Calle: loans.people_additional_data
# Numero Exterior 
# Numero Interior
# Colonia: loans.people_additional_data
# Delegacion o Municipio: loans.people_additional_data
# Ciudad: loans.people_additional_data
# Estado: loans.people_additional_data
# Codigo Postal: loans.people_additional_data
# Clave de Usuario que reporta el credito: TBD by CB
# Nombre del Usuario que reporta el credito: "Tala Mobile"
# Numero de Credito Vigente
# Tipo de Responsabilidad
# Tipo Cuenta
# Tipo Contrato
# Moneda
# Frecuencia de Pago: loans.loan_type - repayment_frequency
# Monto de Pago: loans.loan_type - amount
# Fecha de Apertura: loans.loan_funding - timestamp
# Fecha del Ultimo Pago: loans.loan_status - last_modified
# Fecha ultima compra: loans.loan_funding - timestamp
# Fecha Corte: loans.loan_repayment date_time
# Credito Maximo Autorizado: loans.loan - amount
# Saldo Actual: loans.loan - still_owed
# Limite Crédito: loans.people_credit_limit - credit_limit
# Pago Actual: 
# Saldo Insoluto del Principal: if status = 

source('helper.R')

## sample time period. tend = "fecha corte"
tstart <- '2017-01-01 00:00:00'
tend <- '2017-06-01 00:00:00'

## pull address info from people and people additional table. merge before pulling them 
ssh_replica()
people <- get_db('loans', paste("SELECT id, signup_date, name, first_name, last_name, dob, data 
                                FROM people JOIN people_additional_data 
                                ON people.id = people_additional_data.person_id
                                WHERE signup_date >=", shQuote(tstart), "AND signup_date <=", shQuote(tend)))
kill_replica()
people <- people[, date := substr(signup_date, 1, 10)]
people <- data.frame(people)
people <- concat.split(people, split.col = "data", sep = ",", structure = "compact", drop = TRUE)

# Add national ID and address info from data column

# change the column names to match Circulo de Credito report
colnames(new_dat) <- c("RFC",
                        "Primer Nombre",
                        "Segundo y otros Nombres",
                        "Apellido Paterno",
                        "Segundo Apellido",
                        "Fecha de Nacimiento",
                        "Calle","Numero Exterior",
                        "Numero Interior",
                        "Colonia",
                        "Delegacion o Municipio",
                        "Ciudad",
                        "Estado",
                        "Codigo Postal",
                        "Clave de Usuario que reporta el credito",
                        "Nombre del Usuario que reporta el credito",
                        "Numero de Credito Vigente",
                        "Tipo de Responsabilidad",
                        "Tipo Cuenta",
                        "Tipo Contrato",
                        "Moneda",
                        "Frecuencia de Pago",
                        "Monto de Pago",
                        "Fecha de Apertura",
                        "Fecha del Ultimo Pago",
                        "Fecha ultima compra",
                        "Fecha Corte",
                        "Credito Maximo Autorizado",
                        "Saldo Actual",
                        "Limite Crédito",
                        "Pago Actual",
                        "Saldo Insoluto del Principal")

rownames(new_dat) <- NULL
