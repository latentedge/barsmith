use anyhow::{Result, anyhow};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FormulaOperator {
    GreaterThan,
    LessThan,
    GreaterEqual,
    LessEqual,
    Equal,
    NotEqual,
}

impl FormulaOperator {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            ">" => Some(Self::GreaterThan),
            "<" => Some(Self::LessThan),
            ">=" => Some(Self::GreaterEqual),
            "<=" => Some(Self::LessEqual),
            "=" | "==" => Some(Self::Equal),
            "!=" => Some(Self::NotEqual),
            _ => None,
        }
    }

    pub fn symbol(self) -> &'static str {
        match self {
            Self::GreaterThan => ">",
            Self::LessThan => "<",
            Self::GreaterEqual => ">=",
            Self::LessEqual => "<=",
            Self::Equal => "==",
            Self::NotEqual => "!=",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct FormulaClause {
    pub raw: String,
    pub left: String,
    pub operator: Option<FormulaOperator>,
    pub right: Option<String>,
}

impl FormulaClause {
    pub fn is_flag(&self) -> bool {
        self.operator.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RankedFormula {
    pub rank: usize,
    pub expression: String,
    pub clauses: Vec<FormulaClause>,
}

impl RankedFormula {
    pub fn depth(&self) -> usize {
        self.clauses.len()
    }
}

pub fn parse_ranked_formulas(text: &str) -> Result<Vec<RankedFormula>> {
    let mut formulas = Vec::new();

    for (line_idx, raw_line) in text.lines().enumerate() {
        let line_number = line_idx + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (rank, expression) = parse_rank_and_expression(line, formulas.len() + 1)
            .map_err(|error| anyhow!("formula line {line_number}: {error}"))?;
        let clauses = parse_expression(expression)
            .map_err(|error| anyhow!("formula line {line_number}: {error}"))?;

        formulas.push(RankedFormula {
            rank,
            expression: expression.to_string(),
            clauses,
        });
    }

    if formulas.is_empty() {
        return Err(anyhow!("formula file did not contain any formulas"));
    }

    Ok(formulas)
}

fn parse_rank_and_expression(line: &str, fallback_rank: usize) -> Result<(usize, &str)> {
    let Some(rest) = line.strip_prefix("Rank ") else {
        return Ok((fallback_rank, line));
    };

    let Some((rank_part, expression)) = rest.split_once(':') else {
        return Err(anyhow!("ranked formula is missing ':' separator"));
    };

    let rank = rank_part
        .trim()
        .parse::<usize>()
        .map_err(|_| anyhow!("invalid rank '{}'", rank_part.trim()))?;
    let expression = expression.trim();
    if expression.is_empty() {
        return Err(anyhow!("ranked formula has an empty expression"));
    }

    Ok((rank, expression))
}

pub fn parse_expression(expression: &str) -> Result<Vec<FormulaClause>> {
    let mut clauses = Vec::new();
    for raw_clause in expression.split("&&") {
        let raw = raw_clause.trim();
        if raw.is_empty() {
            continue;
        }
        clauses.push(parse_clause(raw)?);
    }

    if clauses.is_empty() {
        return Err(anyhow!("formula expression has no clauses"));
    }

    Ok(clauses)
}

fn parse_clause(raw: &str) -> Result<FormulaClause> {
    const OPERATORS: [&str; 7] = ["<=", ">=", "==", "!=", "<", ">", "="];

    for op in OPERATORS {
        if let Some(pos) = raw.find(op) {
            let left = raw[..pos].trim();
            let right = raw[pos + op.len()..].trim();
            if left.is_empty() || right.is_empty() {
                return Err(anyhow!("invalid clause '{raw}'"));
            }
            let operator = FormulaOperator::parse(op)
                .ok_or_else(|| anyhow!("unsupported operator '{op}' in clause '{raw}'"))?;
            return Ok(FormulaClause {
                raw: raw.to_string(),
                left: left.to_string(),
                operator: Some(operator),
                right: Some(right.to_string()),
            });
        }
    }

    Ok(FormulaClause {
        raw: raw.to_string(),
        left: raw.to_string(),
        operator: None,
        right: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ranked_formulas() {
        let formulas = parse_ranked_formulas(
            r#"
            # comment
            Rank 7: is_kf_positive_surprise && rsi_7>40.0 && close<high
            Rank 8: flag
            "#,
        )
        .unwrap();

        assert_eq!(formulas.len(), 2);
        assert_eq!(formulas[0].rank, 7);
        assert_eq!(formulas[0].clauses.len(), 3);
        assert_eq!(formulas[0].clauses[1].left, "rsi_7");
        assert_eq!(
            formulas[0].clauses[1].operator,
            Some(FormulaOperator::GreaterThan)
        );
        assert_eq!(formulas[0].clauses[1].right.as_deref(), Some("40.0"));
        assert!(formulas[1].clauses[0].is_flag());
    }

    #[test]
    fn supports_unranked_formulas_and_equality() {
        let formulas = parse_ranked_formulas("trend==1 && side!=0 && x=y").unwrap();

        assert_eq!(formulas[0].rank, 1);
        assert_eq!(
            formulas[0].clauses[0].operator,
            Some(FormulaOperator::Equal)
        );
        assert_eq!(
            formulas[0].clauses[1].operator,
            Some(FormulaOperator::NotEqual)
        );
        assert_eq!(
            formulas[0].clauses[2].operator,
            Some(FormulaOperator::Equal)
        );
    }

    #[test]
    fn rejects_empty_files_and_bad_rank_lines() {
        assert!(parse_ranked_formulas("\n# nope\n").is_err());
        assert!(parse_ranked_formulas("Rank nope: flag").is_err());
        assert!(parse_ranked_formulas("Rank 1:").is_err());
    }
}
