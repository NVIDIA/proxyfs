# icert

A simplification of tools like `openssl` to create either a CA Certificate
or an Endpoint Certificate using either the `Ed25519` or `RSA` key generation
method.

## Usage
```
  -ca
    	generated CA Certicate usable for signing Endpoint Certificates
  -caCert string
    	path to CA Certificate
  -caKey string
    	path to CA Certificate's PrivateKey
  -cert string
    	path to Endpoint Certificate
  -country value
    	generated Certificate's Subject.Country
  -dns value
    	generated Certificate's DNS Name
  -ed25519
    	generate key via Ed25519
  -ip value
    	generated Certificate's IP Address
  -key string
    	path to Endpoint Certificate's PrivateKey
  -locality value
    	generated Certificate's Subject.Locality
  -organization value
    	generated Certificate's Subject.Organization
  -postalCode value
    	generated Certificate's Subject.PostalCode
  -province value
    	generated Certificate's Subject.Province
  -rsa
    	generate key via RSA
  -streetAddress value
    	generated Certificate's Subject.StreetAddress
  -ttl duration
    	generated Certificate's time to live
  -v	verbose mode
```

Precisely one of `-ed25519` or `-rsa` must be specified.

A `-ttl` must be specified.

Both `-caCert` and `-caKey` must be specified.

If `-ca` is specified:
* neither `-cert` nor `-key` may be specified
* neither `-dns` nor `-ip` may be specified

If `-ca` is not specified:
* both `-cert` and `-key` must be specified
* at least one `-dns` and/or one `-ip` must be specified
