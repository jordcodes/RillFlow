use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, spanned::Spanned, Attribute, Data, DeriveInput,
    Error, ExprPath, Lit, Meta, MetaNameValue, Result, Token,
};

#[proc_macro_derive(Upcaster, attributes(upcaster))]
pub fn derive_upcaster(input: TokenStream) -> TokenStream {
    match impl_upcaster(parse_macro_input!(input as DeriveInput)) {
        Ok(tokens) => tokens,
        Err(err) => err.to_compile_error().into(),
    }
}

fn impl_upcaster(input: DeriveInput) -> Result<TokenStream> {
    if !matches!(input.data, Data::Struct(_)) {
        return Err(Error::new(
            input.span(),
            "#[derive(Upcaster)] only supports structs",
        ));
    }

    let config = Config::from_attrs(&input.attrs)?;

    let name = input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let from_type = config.from_type.ok_or_else(|| attr_error("from_type"))?;
    let to_type = config.to_type.ok_or_else(|| attr_error("to_type"))?;
    let from_version = config
        .from_version
        .ok_or_else(|| attr_error("from_version"))?;
    let to_version = config.to_version.ok_or_else(|| attr_error("to_version"))?;
    let transform = config.transform.ok_or_else(|| attr_error("transform"))?;

    Ok(quote! {
        impl #impl_generics ::rillflow::upcasting::Upcaster for #name #ty_generics #where_clause {
            fn from_type(&self) -> &str {
                #from_type
            }

            fn from_version(&self) -> i32 {
                #from_version
            }

            fn to_type(&self) -> &str {
                #to_type
            }

            fn to_version(&self) -> i32 {
                #to_version
            }

            fn upcast(&self, body: &::serde_json::Value) -> ::rillflow::Result<::serde_json::Value> {
                (#transform)(body)
            }
        }
    }
    .into())
}

struct Config {
    from_type: Option<syn::LitStr>,
    to_type: Option<syn::LitStr>,
    from_version: Option<i32>,
    to_version: Option<i32>,
    transform: Option<ExprPath>,
}

impl Config {
    fn from_attrs(attrs: &[Attribute]) -> Result<Self> {
        let mut cfg = Config {
            from_type: None,
            to_type: None,
            from_version: None,
            to_version: None,
            transform: None,
        };

        for attr in attrs {
            if !attr.path().is_ident("upcaster") {
                continue;
            }
            let metas: Punctuated<Meta, Token![,]> =
                attr.parse_args_with(Punctuated::parse_terminated)?;
            for meta in metas {
                match meta {
                    Meta::NameValue(MetaNameValue { path, value, .. }) => {
                        if path.is_ident("from_type") {
                            cfg.from_type = Some(expect_str(value, "from_type")?);
                        } else if path.is_ident("to_type") {
                            cfg.to_type = Some(expect_str(value, "to_type")?);
                        } else if path.is_ident("from_version") {
                            cfg.from_version = Some(expect_int(value, "from_version")?);
                        } else if path.is_ident("to_version") {
                            cfg.to_version = Some(expect_int(value, "to_version")?);
                        } else if path.is_ident("transform") {
                            cfg.transform = Some(expect_path(value, "transform")?);
                        } else {
                            return Err(Error::new(
                                path.span(),
                                "unsupported upcaster attribute key",
                            ));
                        }
                    }
                    other => {
                        return Err(Error::new(
                            other.span(),
                            "expected `key = value` pairs inside #[upcaster(...)]",
                        ));
                    }
                }
            }
        }

        Ok(cfg)
    }
}

fn expect_str(expr: syn::Expr, name: &str) -> Result<syn::LitStr> {
    match expr {
        syn::Expr::Lit(expr_lit) => {
            if let Lit::Str(s) = expr_lit.lit {
                Ok(s)
            } else {
                Err(Error::new(
                    expr_lit.span(),
                    format!("{name} must be a string literal"),
                ))
            }
        }
        other => Err(Error::new(
            other.span(),
            format!("{name} must be a string literal"),
        )),
    }
}

fn expect_int(expr: syn::Expr, name: &str) -> Result<i32> {
    match expr {
        syn::Expr::Lit(expr_lit) => {
            if let Lit::Int(ref int) = expr_lit.lit {
                int.base10_parse::<i32>().map_err(|_| {
                    Error::new(
                        expr_lit.span(),
                        format!("{name} must be an integer literal"),
                    )
                })
            } else {
                Err(Error::new(
                    expr_lit.span(),
                    format!("{name} must be an integer literal"),
                ))
            }
        }
        other => Err(Error::new(
            other.span(),
            format!("{name} must be an integer literal"),
        )),
    }
}

fn expect_path(expr: syn::Expr, name: &str) -> Result<ExprPath> {
    match expr {
        syn::Expr::Lit(expr_lit) => {
            if let Lit::Str(ref path) = expr_lit.lit {
                syn::parse_str::<ExprPath>(&path.value())
                    .map_err(|_| Error::new(path.span(), format!("{name} must be a function path")))
            } else {
                Err(Error::new(
                    expr_lit.span(),
                    format!("{name} must be a string literal path"),
                ))
            }
        }
        other => Err(Error::new(
            other.span(),
            format!("{name} must be a string literal path"),
        )),
    }
}

fn attr_error(attr: &str) -> Error {
    Error::new(
        proc_macro2::Span::call_site(),
        format!("missing `{attr}` in #[upcaster(...)]"),
    )
}
