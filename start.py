from for_linkedin import main as main_linkedin

# Primero se determina qué dataset generar.
print("Opciones.\n" +
    "1- Generar dataset para LinkedIn\n" +
    "2- Generar dataset para Computrabajo\n" +
    "3- Generar dataset para Ineed\n" +
    "4- Generar dataset para Accionempleo\n" +
    "5- Generar todos los dataset\n")
opcion = int(input("Ingrese un número correspondiente a una de las opciones: "))

# En caso de que la opción escogida no sea la adecuada,
# se le solicita al usuario que ingrese nuevamente un valor
# entre 1 y 5.
while (opcion < 1 or opcion > 5):
    opcion = int(input("Por favor seleccione una opción entre 1 y 5: "))

if (opcion == 1):
    main_linkedin()
elif (opcion == 2):
    pass
elif (opcion == 3):
    pass
elif (opcion == 4):
    pass
else:
    pass


