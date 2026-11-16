//! `#[atoll_entry]`: an entry into Atoll (docs/internals/atoll/DESIGN.md 22.11) runs in a frame.
//!
//! The function's body becomes `let __atoll_frame = Frame::enter();` followed by
//! `Frame::run((owned arguments), move |(their patterns)| body)`: entering aborts if the thread
//! is already unwinding, and the frame, dropped last, aborts if a panic unwinds out of the body.
//! Every parameter the function owns by value is renamed in the signature and passed to the
//! closure that runs the body, as that closure's parameter, with its pattern, `mut` included; a
//! pattern of `_` binds a name of the attribute's instead, so the value is held as a named one
//! is. A parameter taken by reference is the caller's.
//!
//! So an entry drops what it owns in the order a function does, all of it inside the frame,
//! whether the body uses a parameter, only borrows it or ignores it: the body's locals, then the
//! temporaries of its last expression (a lock's guard, say), then its owned parameters, the last
//! bound first, then the frame. Parts of a parameter its pattern leaves unbound (`..`, a `_`
//! inside it) drop after every binding. The body's value is made where the entry returns it.
//!
//! Every name the attribute adds is hygienic (`Span::mixed_site`): no parameter or binding of the
//! function can shadow one, or be shadowed by one. Two forms are refused, as their owned values
//! would drop outside the frame: a receiver taken by value, dropped after the frame's locals, and
//! an `async fn`, whose future owns the parameters before its body, and so the frame, ever runs.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{spanned::Spanned, FnArg, ItemFn, Pat, ReturnType, Type};

pub fn atoll_entry(
    attr: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    expand(attr.into(), input.into()).into()
}

fn expand(attr: TokenStream, input: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        return syn::Error::new(Span::call_site(), "#[atoll_entry] takes no arguments")
            .to_compile_error();
    }
    let mut function = match syn::parse2::<ItemFn>(input) {
        Ok(function) => function,
        Err(e) => return e.to_compile_error(),
    };
    if let Some(asyncness) = function.sig.asyncness {
        return syn::Error::new(
            asyncness.span(),
            "an Atoll entry may not be async: its future would own the parameters before the \
             frame is entered, and dropping it unpolled would drop them outside any frame",
        )
        .to_compile_error();
    }
    let hygienic = |name: String| format_ident!("{}", name, span = Span::mixed_site());
    let (mut args, mut patterns) = (Vec::new(), Vec::new());
    for (n, arg) in function.sig.inputs.iter_mut().enumerate() {
        match arg {
            FnArg::Receiver(receiver) => {
                if receiver.reference.is_none() && !is_reference(&receiver.ty) {
                    return syn::Error::new(
                        receiver.span(),
                        "an Atoll entry may not take its receiver by value: it would be dropped \
                         after the frame",
                    )
                    .to_compile_error();
                }
            }
            FnArg::Typed(typed) => {
                if is_reference(&typed.ty) {
                    continue;
                }
                let arg = hygienic(format!("__atoll_arg{n}"));
                let pat = match std::mem::replace(&mut *typed.pat, Pat::Verbatim(quote! { #arg })) {
                    Pat::Wild(_) => {
                        let ignored = hygienic(format!("__atoll_ignored{n}"));
                        Pat::Verbatim(quote! { #ignored })
                    }
                    pat => pat,
                };
                args.push(arg);
                patterns.push(pat);
            }
        }
    }
    let frame = hygienic("__atoll_frame".into());
    let body = &function.block;
    let output = match &function.sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };
    // The body is the closure's, its owned parameters the closure's own, and the closure the
    // block's tail: its value is the call's, made where the function returns it (bound to a local
    // and returned after, a large value would be copied there). Rebound inside the closure, the
    // parameters would be its locals, dropped before its last expression's temporaries.
    function.block = syn::parse_quote!({
        let #frame = crate::storage::atoll::unwind::Frame::enter();
        crate::storage::atoll::unwind::Frame::run(
            (#(#args,)*),
            move |#[allow(unused_variables, unused_mut)] (#(#patterns,)*)| -> #output #body,
        )
    });
    // A body that only ever returns from inside a loop, or that panics, has type `!`, which makes
    // the binding's use unreachable and the binding a diverging sub-expression; that is the
    // body's shape, not a fault.
    function.attrs.push(syn::parse_quote!(
        #[allow(unreachable_code, clippy::diverging_sub_expression)]
    ));
    quote!(#function)
}

fn is_reference(ty: &Type) -> bool {
    match ty {
        Type::Reference(_) => true,
        Type::Paren(p) => is_reference(&p.elem),
        Type::Group(g) => is_reference(&g.elem),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(attr: TokenStream, item: TokenStream) -> String {
        let out = expand(attr, item).to_string();
        assert!(out.contains("compile_error"), "accepted: {out}");
        out
    }

    #[test]
    fn an_async_entry_is_refused() {
        let out = refused(
            quote!(),
            quote!(
                async fn f(&self, owned: Owned) {}
            ),
        );
        assert!(out.contains("may not be async"), "{out}");
    }

    #[test]
    fn a_receiver_by_value_is_refused() {
        let out = refused(
            quote!(),
            quote!(
                fn f(self) {}
            ),
        );
        assert!(out.contains("receiver by value"), "{out}");
        refused(
            quote!(),
            quote!(
                fn f(mut self: Box<Self>) {}
            ),
        );
    }

    #[test]
    fn arguments_to_the_attribute_are_refused() {
        refused(
            quote!(x),
            quote!(
                fn f() {}
            ),
        );
    }

    /// The frame opens first; the owned parameters are passed to the closure that runs the body,
    /// as its parameter, each with its pattern and a `_` bound to a name; a parameter taken by
    /// reference stays in the signature.
    #[test]
    fn an_entry_opens_its_frame_and_passes_its_owned_parameters_to_its_body() {
        let out = expand(
            quote!(),
            quote!(
                fn f(&self, a: A, (b, _): (B, C), _: D, r: &R) -> u8 {
                    0
                }
            ),
        )
        .to_string();
        let frame = out.find("Frame :: enter ()").expect("a frame");
        let args = out
            .find("(__atoll_arg1 , __atoll_arg2 , __atoll_arg3 ,)")
            .expect("the owned arguments");
        let patterns = out
            .find("(a , (b , _) , __atoll_ignored3 ,) | -> u8")
            .expect("their patterns, the closure's parameter");
        assert!(frame < args && args < patterns, "{out}");
        assert!(out.contains("r : & R"), "{out}");
        assert!(!out.contains("__atoll_arg4"), "{out}");
    }
}
