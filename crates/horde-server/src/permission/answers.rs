//! Resolving a human's answers against the questions a request asked (W12c).
//!
//! A client types answers loosely — `1=Red`, `colour=2` — because the real
//! question ids are awkward (Claude keys answers by the full question text).
//! Only the Server holds the request's questions, so it resolves the keys and
//! values here, once, and journals the canonical form: every key a question
//! id, every value an option label or permitted free text. What reaches the
//! harness is therefore exactly what it asked for, or the answer is refused
//! before anything is journaled.

use std::collections::HashMap;

use horde_proto::v1::{Answer, Question};

/// Resolves raw answers to canonical ones, or explains the first problem.
///
/// * A key is a question's id, or its 1-based position when no id matches.
/// * A value is an option label, an option's 1-based number when no label
///   matches, or free text where the question allows it.
/// * Every question must be answered, once, with one value unless it is
///   multi-select.
///
/// # Errors
///
/// A sentence naming the key or question at fault, for the refusal.
pub fn resolve_answers(
    questions: &[Question],
    raw: &HashMap<String, Answer>,
) -> Result<HashMap<String, Answer>, String> {
    let mut resolved: HashMap<String, Answer> = HashMap::new();
    // Sorted so the first problem reported is the same on every run.
    let mut keys: Vec<&String> = raw.keys().collect();
    keys.sort();
    for key in keys {
        let question =
            question_for(questions, key).ok_or_else(|| format!("no question matches {key:?}"))?;
        if resolved.contains_key(&question.id) {
            return Err(format!("question {:?} is answered twice", question.id));
        }
        let values = raw
            .get(key)
            .map(|answer| answer.values.as_slice())
            .unwrap_or_default();
        if values.is_empty() {
            return Err(format!("question {:?} has an empty answer", question.id));
        }
        if values.len() > 1 && !question.multi_select {
            return Err(format!(
                "question {:?} takes one answer, got {}",
                question.id,
                values.len()
            ));
        }
        let values = values
            .iter()
            .map(|value| value_for(question, value))
            .collect::<Result<Vec<_>, _>>()?;
        resolved.insert(question.id.clone(), Answer { values });
    }
    if let Some(unanswered) = questions.iter().find(|q| !resolved.contains_key(&q.id)) {
        return Err(format!("question {:?} is unanswered", unanswered.id));
    }
    Ok(resolved)
}

fn question_for<'a>(questions: &'a [Question], key: &str) -> Option<&'a Question> {
    questions.iter().find(|q| q.id == key).or_else(|| {
        key.parse::<usize>()
            .ok()
            .and_then(|position| position.checked_sub(1))
            .and_then(|index| questions.get(index))
    })
}

fn value_for(question: &Question, value: &str) -> Result<String, String> {
    if let Some(option) = question.options.iter().find(|o| o.label == value) {
        return Ok(option.label.clone());
    }
    let numbered = value
        .parse::<usize>()
        .ok()
        .and_then(|number| number.checked_sub(1))
        .and_then(|index| question.options.get(index));
    if let Some(option) = numbered {
        return Ok(option.label.clone());
    }
    if question.allows_free_text || question.options.is_empty() {
        return Ok(value.to_owned());
    }
    Err(format!(
        "{value:?} is not an option of question {:?}; choose one of {}",
        question.id,
        question
            .options
            .iter()
            .map(|o| format!("{:?}", o.label))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use horde_proto::v1::{Answer, Question, QuestionOption};

    use super::resolve_answers;

    fn question(id: &str, options: &[&str], free_text: bool, multi: bool) -> Question {
        Question {
            id: id.to_owned(),
            header: String::new(),
            question: format!("{id}?"),
            options: options
                .iter()
                .map(|label| QuestionOption {
                    label: (*label).to_owned(),
                    description: String::new(),
                })
                .collect(),
            multi_select: multi,
            allows_free_text: free_text,
            secret: false,
        }
    }

    fn raw(pairs: &[(&str, &[&str])]) -> HashMap<String, Answer> {
        pairs
            .iter()
            .map(|(key, values)| {
                (
                    (*key).to_owned(),
                    Answer {
                        values: values.iter().map(|v| (*v).to_owned()).collect(),
                    },
                )
            })
            .collect()
    }

    fn values(resolved: &HashMap<String, Answer>, id: &str) -> Vec<String> {
        resolved
            .get(id)
            .map(|a| a.values.clone())
            .unwrap_or_default()
    }

    #[test]
    fn keys_resolve_by_id_then_by_position_and_values_by_label_then_number() {
        let questions = [
            question(
                "Which colour do you prefer?",
                &["Red", "Blue"],
                false,
                false,
            ),
            question("sizes", &["S", "M", "L"], false, true),
        ];
        let resolved = resolve_answers(&questions, &raw(&[("1", &["2"]), ("sizes", &["S", "3"])]))
            .expect("valid answers");
        assert_eq!(
            values(&resolved, "Which colour do you prefer?"),
            vec!["Blue"]
        );
        assert_eq!(values(&resolved, "sizes"), vec!["S", "L"]);
        assert_eq!(resolved.len(), 2, "keys are canonical question ids");
    }

    /// An id that looks like a number outranks the position it would name.
    #[test]
    fn an_exact_id_wins_over_a_position() {
        let questions = [
            question("x", &[], true, false),
            question("1", &[], true, false),
        ];
        let resolved =
            resolve_answers(&questions, &raw(&[("1", &["one"]), ("x", &["ex"])])).expect("valid");
        assert_eq!(values(&resolved, "1"), vec!["one"]);
        assert_eq!(values(&resolved, "x"), vec!["ex"]);
    }

    #[test]
    fn free_text_is_accepted_only_where_the_question_allows_it() {
        let open = [question("q", &["Red"], true, false)];
        let resolved = resolve_answers(&open, &raw(&[("q", &["Green"])])).expect("free text ok");
        assert_eq!(values(&resolved, "q"), vec!["Green"]);

        let optionless = [question("q", &[], false, false)];
        assert!(resolve_answers(&optionless, &raw(&[("q", &["anything"])])).is_ok());

        let closed = [question("q", &["Red", "Blue"], false, false)];
        let error = resolve_answers(&closed, &raw(&[("q", &["Green"])])).expect_err("refused");
        assert!(error.contains("\"Red\", \"Blue\""), "{error}");
        let error = resolve_answers(&closed, &raw(&[("q", &["3"])])).expect_err("no option 3");
        assert!(error.contains("\"3\""), "{error}");
        let error = resolve_answers(&closed, &raw(&[("q", &["0"])])).expect_err("no option 0");
        assert!(error.contains("\"0\""), "{error}");
    }

    #[test]
    fn every_malformed_answer_is_refused_with_its_reason() {
        let questions = [
            question("a", &["x", "y"], false, false),
            question("b", &[], true, false),
        ];
        let refuse = |pairs: &[(&str, &[&str])]| {
            resolve_answers(&questions, &raw(pairs)).expect_err("must be refused")
        };
        assert!(refuse(&[("zzz", &["x"]), ("b", &["t"])]).contains("no question matches \"zzz\""));
        assert!(refuse(&[("0", &["x"]), ("b", &["t"])]).contains("no question matches \"0\""));
        assert!(refuse(&[("3", &["x"]), ("b", &["t"])]).contains("no question matches \"3\""));
        assert!(refuse(&[("a", &["x"])]).contains("question \"b\" is unanswered"));
        assert!(refuse(&[("a", &["x", "y"]), ("b", &["t"])]).contains("takes one answer, got 2"));
        assert!(refuse(&[("a", &[]), ("b", &["t"])]).contains("empty answer"));
        assert!(refuse(&[("a", &["x"]), ("1", &["y"]), ("b", &["t"])]).contains("answered twice"));
    }
}
