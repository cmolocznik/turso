//! `#[atoll_entry]`: an entry into Atoll (docs/internals/atoll/DESIGN.md 22.11) runs in a frame.
//!
//! The body begins with `let __atoll_frame = crate::storage::atoll::unwind::Frame::enter();`:
//! entering aborts if the thread is already unwinding, and the frame, dropped last, aborts if a
//! panic unwinds out of the body. Every parameter the function owns by value is renamed in the
//! signature and moved into a local of the body declared after the frame, so it drops inside the
//! frame whether the body uses it, only borrows it or ignores it; its pattern, `_` and `mut`
//! included, binds from that local. A parameter taken by reference is the caller's.
//!
//! Every name the attribute adds is hygienic (`Span::mixed_site`): no parameter or binding of the
//! function can shadow one, or be shadowed by one. Two forms are refused, as their owned values
//! would drop outside the frame: a receiver taken by value, dropped after the frame's locals, and
//! an `async fn`, whose future owns the parameters before its body, and so the frame, ever runs.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{spanned::Spanned, FnArg, ItemFn, Pat, Type};

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
    let mut moved = Vec::new();
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
                let whole = hygienic(format!("__atoll_owned{n}"));
                let pat = std::mem::replace(&mut *typed.pat, Pat::Verbatim(quote! { #arg }));
                moved.push(quote! {
                    let #whole = #arg;
                    #[allow(unused_variables, unused_mut, clippy::let_unit_value)]
                    let #pat = #whole;
                });
            }
        }
    }
    let (frame, out) = (
        hygienic("__atoll_frame".into()),
        hygienic("__atoll_out".into()),
    );
    let body = &function.block;
    // The body's value is bound first, so the temporaries of its last expression (a lock's
    // guard, say) drop at the end of that statement, inside the frame: as the tail of a block
    // they would outlive the block's locals, the frame among them.
    function.block = syn::parse_quote!({
        let #frame = crate::storage::atoll::unwind::Frame::enter();
        #(#moved)*
        let #out = #body;
        #out
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

    /// The frame opens first, each owned parameter is moved into the body after it, and a
    /// parameter taken by reference stays in the signature.
    #[test]
    fn an_entry_opens_its_frame_before_it_takes_its_parameters() {
        let out = expand(
            quote!(),
            quote!(
                fn f(&self, a: A, (b, _): (B, C), r: &R) -> u8 {
                    0
                }
            ),
        )
        .to_string();
        let frame = out.find("Frame :: enter ()").expect("a frame");
        let first = out
            .find("let __atoll_owned1 = __atoll_arg1")
            .expect("a moved");
        let pattern = out
            .find("let (b , _) = __atoll_owned2")
            .expect("a pattern bound");
        assert!(frame < first && first < pattern, "{out}");
        assert!(out.contains("r : & R"), "{out}");
        assert!(!out.contains("__atoll_arg3"), "{out}");
    }
}
