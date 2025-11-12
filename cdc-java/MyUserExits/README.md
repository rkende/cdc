# CDC Java User Exits

IBM InfoSphere Data Replication (CDC) 11.4.0.5-5770

## Estructura
```src/main/java/com/ibm/cdc/userexits/
├── UserExitWait.java  → Espera 5-10 segundos aleatorios
└── KcopWait.java      → KCOP con delay para Kafka
```

## Build
```bash
mvn clean package
```

## Deploy
Copy `target/my-user-exits-1.0.0.jar` to folder `<CDC_Target_Instance>\lib\`

## Configuration
Management Console → User Exits → UserExitWait  
Kafka Topic Mappings → KCOP → KcopWait

