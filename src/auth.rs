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
    service: String,
    hash: String,
    roles: Vec<Role>,
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
#[derive(Object)]
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
    pub fn get_service(&self) -> &str {
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
