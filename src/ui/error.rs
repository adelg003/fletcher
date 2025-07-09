use crate::ui::layout::base_layout;
use maud::{Markup, html};
use poem::{
    Response,
    error::NotFoundError,
    http::{HeaderValue, StatusCode},
};

const ASCII_404: &str = r"
   __ __  ____  __ __              ____                      _   __      __     ______                      __
  / // / / __ \/ // /             / __ \____ _____ ____     / | / /___  / /_   / ____/___  __  ______  ____/ /
 / // /_/ / / / // /_   ______   / /_/ / __ `/ __ `/ _ \   /  |/ / __ \/ __/  / /_  / __ \/ / / / __ \/ __  /
/__  __/ /_/ /__  __/  /_____/  / ____/ /_/ / /_/ /  __/  / /|  / /_/ / /_   / __/ / /_/ / /_/ / / / / /_/ /
  /_/  \____/  /_/             /_/    \__,_/\__, /\___/  /_/ |_/\____/\__/  /_/    \____/\__,_/_/ /_/\__,_/
                                           /____/
";

const ASCII_COW_SAY: &str = r#"
 ___________________________________________________________
/ What are you...                                           \
\ There's no Mars bars down here, what are you looking for? /
 \    ------------------------------------------------------
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

/// 404 page
pub async fn not_found_404(_: NotFoundError) -> Response {
    let body: Markup = base_layout(
        "(╯°□°)╯︵ ɹoɹɹƎ",
        &None,
        html! {
            pre { (ASCII_404) }
            pre { (ASCII_COW_SAY) }
        },
    );

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(
            "Content-Type",
            HeaderValue::from_static("text/html; charset=utf-8"),
        )
        .body(body.into_string())
}
