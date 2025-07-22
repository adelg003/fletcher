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
