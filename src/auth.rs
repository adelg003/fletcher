use crate::{
    Config,
    error::{Error, Result},
};
use bcrypt::verify;
use jsonwebtoken::{Header, TokenData, Validation, decode, encode, get_current_timestamp};
use poem::Request;
use poem_openapi::{Enum, Object, SecurityScheme, auth::Bearer};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Roles a service can use
#[derive(Clone, Debug, Deserialize, Enum, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
#[oai(rename_all = "lowercase")]
pub enum Role {
    Disable,
    Pause,
    Publish,
    Update,
}

impl fmt::Display for Role {
    /// How to format Role when presented in errors
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text: &str = match self {
            Role::Disable => "disable",
            Role::Pause => "pause",
            Role::Publish => "publish",
            Role::Update => "update",
        };
        write!(formatter, "{text}")
    }
}

/// Struct to hold API cred configs
#[derive(Clone, Deserialize)]
pub struct RemoteAuth {
    pub service: String,
    hash: String,
    pub roles: Vec<Role>,
}

/// Struct for how a remote API will pass in its creds
#[derive(Object)]
pub struct RemoteLogin {
    service: String,
    key: String,
}

const ONE_HOUR: u64 = 60 * 60;

/// Claim for JWT
#[derive(Deserialize, Serialize)]
struct Claim {
    sub: String,
    iat: u64,
    exp: u64,
    iss: String,
    roles: Vec<Role>,
}

/// Authenticated
#[derive(Debug, Object)]
pub struct Authenticated {
    access_token: String,
    issued: u64,
    issued_by: String,
    expires: u64,
    roles: Vec<Role>,
    service: String,
    token_type: String,
    ttl: u64,
}

/// Authenticate a user's key and return a JWT if valid
pub fn authenticate(cred: &RemoteLogin, config: &Config) -> Result<Authenticated> {
    // Pull the config for the requesting API
    let remote_auth: &RemoteAuth = config
        .remote_auths
        .iter()
        .find(|auth: &&RemoteAuth| auth.service == cred.service)
        .ok_or(Error::InvalidService(cred.service.to_string()))?;

    // Does the key match our key hash?
    if !verify(&cred.key, &remote_auth.hash)? {
        return Err(Error::InvalidKey);
    }

    // Shared Parameters
    let issued_by: String = "Fletcher".to_string();
    let service: String = cred.service.to_string();
    let roles: Vec<Role> = remote_auth.roles.to_vec();

    // TTL Parameters
    let issued: u64 = get_current_timestamp();
    let ttl: u64 = ONE_HOUR;
    let expires: u64 = issued + ttl;

    // Claims for the JWT
    let claim = Claim {
        sub: service.clone(),
        iat: issued,
        exp: expires,
        iss: issued_by.clone(),
        roles: roles.clone(),
    };

    // Generate the JWT
    let access_token: String = encode(&Header::default(), &claim, &config.encoding_key)?;

    Ok(Authenticated {
        access_token,
        issued,
        issued_by,
        expires,
        roles,
        service,
        token_type: "Bearer".to_string(),
        ttl,
    })
}

/// Bearer JWT Authentication/// Token Authentication
#[derive(SecurityScheme)]
#[oai(ty = "bearer", checker = "jwt_checker")]
pub struct JwtAuth(RemoteAuth);

impl JwtAuth {
    /// Provide which service is making the request
    pub fn service(&self) -> &str {
        &self.0.service
    }

    /// Check to see if the desired role is present
    pub fn check_role(&self, role: Role) -> Result<()> {
        if self.0.roles.contains(&role) {
            Ok(())
        } else {
            Err(Error::Role(self.0.service.clone(), role))
        }
    }
}

/// How JwtAuth validates a JWT
async fn jwt_checker(req: &Request, bearer: Bearer) -> poem::Result<RemoteAuth> {
    // Pull Fletcher Configs
    let config: &Config = req
        .data::<Config>()
        .ok_or(Error::Unreachable.into_poem_error())?;

    // Pull jwt data
    let jwt: TokenData<Claim> =
        decode::<Claim>(&bearer.token, &config.decoding_key, &Validation::default())
            .map_err(|err| Error::Jwt(err).into_poem_error())?;

    // Make sure the user in the token is still valid.
    let auth: &RemoteAuth = config
        .remote_auths
        .iter()
        .find(|auth: &&RemoteAuth| auth.service == jwt.claims.sub)
        .ok_or(Error::InvalidService(jwt.claims.sub).into_poem_error())?;

    // Return User from inside the token
    Ok(auth.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::load_config;
    use pretty_assertions::assert_eq;

    // =============== Basic Enum Tests ===============

    /// Test Role Display implementation
    #[test]
    fn test_role_display() {
        assert_eq!(Role::Disable.to_string(), "disable");
        assert_eq!(Role::Pause.to_string(), "pause");
        assert_eq!(Role::Publish.to_string(), "publish");
        assert_eq!(Role::Update.to_string(), "update");
    }

    // =============== authenticate() Function Tests ===============

    /// Test authenticate with valid local service credentials
    #[test]
    fn test_authenticate_valid_local() {
        let config = load_config().unwrap();
        let cred = RemoteLogin {
            service: "local".to_string(),
            key: "abc123".to_string(),
        };

        let result = authenticate(&cred, &config);
        assert!(result.is_ok());

        let auth = result.unwrap();
        assert_eq!(auth.service, "local");
        assert_eq!(auth.token_type, "Bearer");
        assert_eq!(auth.issued_by, "Fletcher");
        assert_eq!(auth.ttl, ONE_HOUR);
        assert!(auth.expires > auth.issued);
        assert!(!auth.access_token.is_empty());

        // local service should have all roles
        assert!(auth.roles.contains(&Role::Disable));
        assert!(auth.roles.contains(&Role::Pause));
        assert!(auth.roles.contains(&Role::Publish));
        assert!(auth.roles.contains(&Role::Update));
    }

    /// Test authenticate with valid readonly service credentials
    #[test]
    fn test_authenticate_valid_readonly() {
        let config = load_config().unwrap();
        let cred = RemoteLogin {
            service: "readonly".to_string(),
            key: "abc123".to_string(),
        };

        let result = authenticate(&cred, &config);
        assert!(result.is_ok());

        let auth = result.unwrap();
        assert_eq!(auth.service, "readonly");
        assert_eq!(auth.token_type, "Bearer");
        assert_eq!(auth.issued_by, "Fletcher");
        assert_eq!(auth.ttl, ONE_HOUR);
        assert!(auth.expires > auth.issued);
        assert!(!auth.access_token.is_empty());

        // readonly service should have no roles
        assert!(auth.roles.is_empty());
    }

    /// Test authenticate with invalid service
    #[test]
    fn test_authenticate_invalid_service() {
        let config = load_config().unwrap();
        let cred = RemoteLogin {
            service: "nonexistent".to_string(),
            key: "abc123".to_string(),
        };

        let result = authenticate(&cred, &config);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::InvalidService(ref service) if service == "nonexistent")
        );
    }

    /// Test authenticate with invalid key
    #[test]
    fn test_authenticate_invalid_key() {
        let config = load_config().unwrap();
        let cred = RemoteLogin {
            service: "local".to_string(),
            key: "wrong_password".to_string(),
        };

        let result = authenticate(&cred, &config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidKey));
    }

    /// Test authenticate with empty key
    #[test]
    fn test_authenticate_empty_key() {
        let config = load_config().unwrap();
        let cred = RemoteLogin {
            service: "local".to_string(),
            key: "".to_string(),
        };

        let result = authenticate(&cred, &config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidKey));
    }

    // =============== JwtAuth Struct Tests ===============

    /// Test JwtAuth service method
    #[test]
    fn test_jwt_auth_service() {
        let remote_auth = RemoteAuth {
            service: "test_service".to_string(),
            hash: "dummy_hash".to_string(),
            roles: vec![Role::Publish],
        };

        let jwt_auth = JwtAuth(remote_auth);
        assert_eq!(jwt_auth.service(), "test_service");
    }

    /// Test JwtAuth check_role with valid role
    #[test]
    fn test_jwt_auth_check_role_valid() {
        let remote_auth = RemoteAuth {
            service: "test_service".to_string(),
            hash: "dummy_hash".to_string(),
            roles: vec![Role::Publish, Role::Update],
        };

        let jwt_auth = JwtAuth(remote_auth);

        // Should succeed for roles the user has
        assert!(jwt_auth.check_role(Role::Publish).is_ok());
        assert!(jwt_auth.check_role(Role::Update).is_ok());
    }

    /// Test JwtAuth check_role with invalid role
    #[test]
    fn test_jwt_auth_check_role_invalid() {
        let remote_auth = RemoteAuth {
            service: "test_service".to_string(),
            hash: "dummy_hash".to_string(),
            roles: vec![Role::Publish], // Only has Publish role
        };

        let jwt_auth = JwtAuth(remote_auth);

        // Should fail for roles the user doesn't have
        let result = jwt_auth.check_role(Role::Disable);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::Role(ref service, role) if service == "test_service" && role == Role::Disable)
        );
    }

    /// Test JwtAuth check_role with no roles
    #[test]
    fn test_jwt_auth_check_role_no_roles() {
        let remote_auth = RemoteAuth {
            service: "readonly_service".to_string(),
            hash: "dummy_hash".to_string(),
            roles: vec![], // No roles
        };

        let jwt_auth = JwtAuth(remote_auth);

        // Should fail for any role check
        let result = jwt_auth.check_role(Role::Publish);
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::Role(ref service, role) if service == "readonly_service" && role == Role::Publish)
        );
    }

    // =============== JWT Integration Tests ===============

    /// Test JWT validation logic (integration test with authenticate)
    #[test]
    fn test_jwt_validation_integration() {
        let config = load_config().unwrap();

        // Create a valid JWT using authenticate function
        let cred = RemoteLogin {
            service: "local".to_string(),
            key: "abc123".to_string(),
        };

        let auth_result = authenticate(&cred, &config).unwrap();
        let token = auth_result.access_token;

        // Verify the JWT can be decoded with the same config
        let decoded = jsonwebtoken::decode::<Claim>(
            &token,
            &config.decoding_key,
            &jsonwebtoken::Validation::default(),
        );

        assert!(decoded.is_ok());
        let jwt_data = decoded.unwrap();
        assert_eq!(jwt_data.claims.sub, "local");
        assert_eq!(jwt_data.claims.iss, "Fletcher");
        assert!(jwt_data.claims.roles.contains(&Role::Publish));
    }

    /// Test JWT with invalid token format
    #[test]
    fn test_jwt_invalid_token_format() {
        let config = load_config().unwrap();

        // Try to decode an invalid JWT
        let decoded = jsonwebtoken::decode::<Claim>(
            "invalid.jwt.token",
            &config.decoding_key,
            &jsonwebtoken::Validation::default(),
        );

        assert!(decoded.is_err());
    }

    /// Test JWT creation and validation cycle
    #[test]
    fn test_jwt_creation_validation_cycle() {
        let config = load_config().unwrap();

        // Test both local and readonly services
        let services = vec![
            (
                "local",
                vec![Role::Disable, Role::Pause, Role::Publish, Role::Update],
            ),
            ("readonly", vec![]),
        ];

        for (service_name, expected_roles) in services {
            let cred = RemoteLogin {
                service: service_name.to_string(),
                key: "abc123".to_string(),
            };

            // Authenticate and get JWT
            let auth_result = authenticate(&cred, &config).unwrap();

            // Decode the JWT and verify claims
            let decoded = jsonwebtoken::decode::<Claim>(
                &auth_result.access_token,
                &config.decoding_key,
                &jsonwebtoken::Validation::default(),
            )
            .unwrap();

            assert_eq!(decoded.claims.sub, service_name);
            assert_eq!(decoded.claims.iss, "Fletcher");
            assert_eq!(decoded.claims.roles.len(), expected_roles.len());

            for role in expected_roles {
                assert!(decoded.claims.roles.contains(&role));
            }
        }
    }

    // =============== Struct Creation Tests ===============

    /// Test RemoteLogin and Authenticated structs can be created
    #[test]
    fn test_struct_creation() {
        let login = RemoteLogin {
            service: "test".to_string(),
            key: "test_key".to_string(),
        };
        assert_eq!(login.service, "test");
        assert_eq!(login.key, "test_key");

        let auth = Authenticated {
            access_token: "token".to_string(),
            issued: 1000,
            issued_by: "Fletcher".to_string(),
            expires: 2000,
            roles: vec![Role::Publish],
            service: "test".to_string(),
            token_type: "Bearer".to_string(),
            ttl: 1000,
        };
        assert_eq!(auth.service, "test");
        assert_eq!(auth.token_type, "Bearer");
    }
}
