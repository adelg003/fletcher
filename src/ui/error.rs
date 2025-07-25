use crate::ui::layout::base_layout;
use maud::html;
use poem::{
    Response,
    error::NotFoundError,
    http::{HeaderValue, StatusCode},
};
use rand::{rng, seq::IndexedRandom};

const ASCII_404: &str = r"
       __ __  ____  __ __              ____                      _   __      __     ______                      __
      / // / / __ \/ // /             / __ \____ _____ ____     / | / /___  / /_   / ____/___  __  ______  ____/ /
     / // /_/ / / / // /_   ______   / /_/ / __ `/ __ `/ _ \   /  |/ / __ \/ __/  / /_  / __ \/ / / / __ \/ __  /
    /__  __/ /_/ /__  __/  /_____/  / ____/ /_/ / /_/ /  __/  / /|  / /_/ / /_   / __/ / /_/ / /_/ / / / / /_/ /
      /_/  \____/  /_/             /_/    \__,_/\__, /\___/  /_/ |_/\____/\__/  /_/    \____/\__,_/_/ /_/\__,_/
                                               /____/
";

const COW_SAY_404_BODY: &str = r#"
      \   |                       .       .
       \  |                      / `.   .' "
        \ |              .---.  <    > <    >  .---.
         \|              |    \  \ - ~ ~ - /  /    |
             _____          ..-~             ~-..-~
            |     |   \~~~\.'                    `./~~~/
           ---------   \__/                        \__/
          .'  O    \     /               /       \  "
         (_____,    `._.'               |         }  \/~~~/
          `----.          /       }     |        /    \__/
                `-.      |       /      |       /      `. ,~~|
                    ~-.__|      /_ - ~ ^|      /- _      `..-'
                         |     /        |     /     ~-.     `-. _  _  _
                         |_____|        |_____|         ~ - . _ _ _ _ _>
"#;

const COW_SAY_404_HATS: [&str; 2] = [
    r#"
     ___________________________________________________________
    / What are you...                                           \
    \ There's no Mars bars down here, what are you looking for? /
     \    ------------------------------------------------------"#,
    r#"
     _________________________________________________
    / Why do you suppose I just hurled a 404 error in \
    \ response to your request?                       /
     \    --------------------------------------------"#,
];

/// 404 Error page
pub async fn not_found_404(_: NotFoundError) -> Response {
    let mut rng = rng();
    let cow_say_hat: &&str = COW_SAY_404_HATS.choose(&mut rng).unwrap_or(&"");

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(
            "Content-Type",
            HeaderValue::from_static("text/html; charset=utf-8"),
        )
        .body(
            base_layout(
                "(╯°□°)╯︵ ɹoɹɹƎ",
                &None,
                html! {
                    pre class="mockup-code animate-fade" {
                        code { (ASCII_404) }
                        code { { (cow_say_hat) (COW_SAY_404_BODY) } }
                    }
                },
            )
            .into_string(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use poem::error::NotFoundError;
    use pretty_assertions::assert_eq;

    /// Test not_found_404 function returns correct status code
    #[tokio::test]
    async fn test_not_found_404_status_code() {
        let not_found_error = NotFoundError;
        let response = not_found_404(not_found_error).await;

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "not_found_404 should return NOT_FOUND status code"
        );
    }

    /// Test not_found_404 function returns correct content type header
    #[tokio::test]
    async fn test_not_found_404_content_type() {
        let not_found_error = NotFoundError;
        let response = not_found_404(not_found_error).await;

        let content_type = response.headers().get("Content-Type");
        assert!(
            content_type.is_some(),
            "not_found_404 should include Content-Type header"
        );
        assert_eq!(
            content_type.unwrap(),
            "text/html; charset=utf-8",
            "not_found_404 should have correct Content-Type header value"
        );
    }

    /// Test not_found_404 function returns body containing ASCII art
    #[tokio::test]
    async fn test_not_found_404_contains_ascii_art() {
        let not_found_error = NotFoundError;
        let response = not_found_404(not_found_error).await;

        let body = response.into_body().into_string().await.unwrap();
        assert!(
            body.contains(ASCII_404),
            "not_found_404 response body should contain ASCII_404 art"
        );
    }

    /// Test not_found_404 function returns body containing cow say body
    #[tokio::test]
    async fn test_not_found_404_contains_cow_say_body() {
        let not_found_error = NotFoundError;
        let response = not_found_404(not_found_error).await;

        let body = response.into_body().into_string().await.unwrap();

        // Check for distinctive parts of the cow body that we know are present
        assert!(
            body.contains("\\   |                       .       ."),
            "not_found_404 response body should contain first line of cow body"
        );
        assert!(
            body.contains("_____          ..-~             ~-..-~"),
            "not_found_404 response body should contain middle section of cow body"
        );
        assert!(
            body.contains("~ - . _ _ _ _ _"),
            "not_found_404 response body should contain last part of cow body"
        );
    }

    /// Test not_found_404 function returns body containing one of the cow say hats
    #[tokio::test]
    async fn test_not_found_404_contains_cow_say_hat() {
        // Run the test multiple times to increase probability of hitting both hat variants
        for _ in 0..10 {
            let not_found_error = NotFoundError;
            let response = not_found_404(not_found_error).await;

            let body = response.into_body().into_string().await.unwrap();

            // Check for parts of the hats that don't contain HTML special characters
            let contains_hat_1_part =
                body.contains("What are you...") || body.contains("There's no Mars bars down here");
            let contains_hat_2_part = body.contains("Why do you suppose I just hurled a 404 error")
                || body.contains("response to your request?");

            assert!(
                contains_hat_1_part || contains_hat_2_part,
                "not_found_404 response body should contain parts of one of the COW_SAY_404_HATS"
            );
        }
    }

    /// Test not_found_404 function returns body containing page title
    #[tokio::test]
    async fn test_not_found_404_contains_page_title() {
        let not_found_error = NotFoundError;
        let response = not_found_404(not_found_error).await;

        let body = response.into_body().into_string().await.unwrap();
        assert!(
            body.contains("(╯°□°)╯︵ ɹoɹɹƎ"),
            "not_found_404 response body should contain the error page title"
        );
    }

    /// Test not_found_404 function returns body with HTML structure
    #[tokio::test]
    async fn test_not_found_404_html_structure() {
        let not_found_error = NotFoundError;
        let response = not_found_404(not_found_error).await;

        let body = response.into_body().into_string().await.unwrap();

        // Check for basic HTML structure elements
        assert!(
            body.contains("<html"),
            "not_found_404 response should contain HTML opening tag"
        );
        assert!(
            body.contains("</html>"),
            "not_found_404 response should contain HTML closing tag"
        );
        assert!(
            body.contains("<pre class=\"mockup-code animate-fade\">"),
            "not_found_404 response should contain pre element with correct classes"
        );
        assert!(
            body.contains("<code>"),
            "not_found_404 response should contain code elements"
        );
    }

    /// Test not_found_404 function returns valid HTML document
    #[tokio::test]
    async fn test_not_found_404_valid_html_document() {
        let not_found_error = NotFoundError;
        let response = not_found_404(not_found_error).await;

        let body = response.into_body().into_string().await.unwrap();

        // Check for DOCTYPE and essential HTML elements
        assert!(
            body.contains("<!DOCTYPE html>"),
            "not_found_404 response should contain DOCTYPE declaration"
        );
        assert!(
            body.contains("<head>"),
            "not_found_404 response should contain head element"
        );
        assert!(
            body.contains("<body>"),
            "not_found_404 response should contain body element"
        );
        assert!(
            body.contains("<title>"),
            "not_found_404 response should contain title element"
        );
    }
}
