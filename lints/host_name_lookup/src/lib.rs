// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#![feature(rustc_private)]

extern crate rustc_ast;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_middle;
extern crate rustc_span;

use std::net::{IpAddr, SocketAddr};

use rustc_ast::LitKind;
use rustc_errors::{Diag, DiagDecorator};
use rustc_hir::def::{DefKind, Res};
use rustc_hir::{Expr, ExprKind, Node};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_middle::ty::{self, GenericArgsRef, Ty, TyCtxt};
use rustc_span::def_id::DefId;

dylint_linting::declare_late_lint! {
    /// ### What it does
    ///
    /// Refuses every call whose argument goes through `ToSocketAddrs`
    /// (std's or tokio's) unless the argument can only be addresses: a
    /// `SocketAddr` of any family, an `(IpAddr, u16)` of any family, or
    /// those behind references, arrays, slices, `Vec` and `Option`. A string
    /// literal that parses as a socket address, or a tuple whose host is a
    /// literal IP, is an address too. Anything else, a user type among them,
    /// is refused.
    ///
    /// ### Why is this bad?
    ///
    /// `ToSocketAddrs` looks a name up with the host resolver. Each leg
    /// chooses who resolves its names, so a name that reaches the host
    /// resolver behind the leg's back leaks it.
    ///
    /// ### Example
    ///
    /// ```rust,ignore
    /// socket.send_to(&packet, "tracker.example:6969")?;
    /// ```
    ///
    /// Use instead:
    ///
    /// ```rust,ignore
    /// let address = dialer.resolve("tracker.example").await?;
    /// socket.send_to(&packet, SocketAddr::new(address, 6969))?;
    /// ```
    pub HOST_NAME_LOOKUP,
    Deny,
    "a name handed to the host resolver through `ToSocketAddrs`"
}

const MESSAGE: &str = "a name reaches the host resolver here; resolve it through the leg's \
                       naming (Dialer::resolve) and dial the address";

impl<'tcx> LateLintPass<'tcx> for HostNameLookup {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        let typeck = cx.typeck_results();
        match expr.kind {
            ExprKind::Call(function, arguments) => {
                let ty::FnDef(callee, generics) = *typeck.expr_ty(function).kind() else {
                    return;
                };
                check_arguments(
                    cx,
                    callee,
                    generics.skip_binder(),
                    arguments.iter().collect(),
                );
            }
            ExprKind::MethodCall(_, receiver, arguments, _) => {
                let Some(callee) = typeck.type_dependent_def_id(expr.hir_id) else {
                    return;
                };
                check_arguments(
                    cx,
                    callee,
                    typeck.node_args(expr.hir_id),
                    std::iter::once(receiver).chain(arguments).collect(),
                );
            }
            // A function passed as a value, as in `.map(TcpStream::connect)`:
            // there is no argument to read, so only its types decide. The
            // callee of a call is checked with its arguments above.
            ExprKind::Path(_) if !is_callee(cx, expr) => {
                let ty::FnDef(callee, generics) = *typeck.expr_ty(expr).kind() else {
                    return;
                };
                let generics = generics.skip_binder();
                if refused_parameters(cx.tcx, callee, generics)
                    .next()
                    .is_some()
                {
                    report(cx, expr);
                }
            }
            _ => {}
        }
    }
}

fn check_arguments<'tcx>(
    cx: &LateContext<'tcx>,
    callee: DefId,
    generics: GenericArgsRef<'tcx>,
    arguments: Vec<&'tcx Expr<'tcx>>,
) {
    for index in refused_parameters(cx.tcx, callee, generics) {
        if let Some(argument) = arguments.get(index)
            && !literal_address(cx, argument)
        {
            report(cx, argument);
        }
    }
}

// The positions of `callee`'s parameters bounded by `ToSocketAddrs` whose
// type at this use can hold a name.
fn refused_parameters<'tcx>(
    tcx: TyCtxt<'tcx>,
    callee: DefId,
    generics: GenericArgsRef<'tcx>,
) -> impl Iterator<Item = usize> + 'tcx {
    let signature = tcx.fn_sig(callee).instantiate_identity().skip_binder();
    signature
        .inputs()
        .iter()
        .enumerate()
        .filter_map(move |(index, parameter)| {
            let ty::Param(parameter) = *peel_references(*parameter).kind() else {
                return None;
            };
            (resolves_names(tcx, callee, parameter)
                && !holds_only_addresses(tcx, generics.type_at(parameter.index as usize)))
            .then_some(index)
        })
}

fn is_callee(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
    matches!(
        cx.tcx.parent_hir_node(expr.hir_id),
        Node::Expr(Expr { kind: ExprKind::Call(function, _), .. }) if function.hir_id == expr.hir_id
    )
}

fn report(cx: &LateContext<'_>, expr: &Expr<'_>) {
    cx.emit_span_lint(
        HOST_NAME_LOOKUP,
        expr.span.source_callsite(),
        DiagDecorator(|diag: &mut Diag<'_, ()>| {
            diag.primary_message(MESSAGE);
        }),
    );
}

fn peel_references(mut ty: Ty<'_>) -> Ty<'_> {
    while let ty::Ref(_, inner, _) = *ty.kind() {
        ty = inner;
    }
    ty
}

// Whether `callee` bounds `parameter` by `ToSocketAddrs`. A trait method's
// own `Self` counts, so `to_socket_addrs` itself is caught.
fn resolves_names<'tcx>(tcx: TyCtxt<'tcx>, callee: DefId, parameter: ty::ParamTy) -> bool {
    tcx.param_env(callee)
        .caller_bounds()
        .iter()
        .filter_map(|clause| clause.as_trait_clause())
        .any(|bound| {
            let bound = bound.skip_binder();
            matches!(*bound.self_ty().kind(), ty::Param(bounded) if bounded == parameter)
                && is_to_socket_addrs(tcx, bound.def_id())
        })
}

fn is_to_socket_addrs(tcx: TyCtxt<'_>, trait_id: DefId) -> bool {
    tcx.item_name(trait_id).as_str() == "ToSocketAddrs"
        && matches!(
            tcx.crate_name(trait_id.krate).as_str(),
            "std" | "core" | "tokio"
        )
}

// Whether every value of `ty` is an address, so nothing is looked up.
fn holds_only_addresses<'tcx>(tcx: TyCtxt<'tcx>, ty: Ty<'tcx>) -> bool {
    match *ty.kind() {
        ty::Ref(_, inner, _) | ty::Array(inner, _) | ty::Slice(inner) => {
            holds_only_addresses(tcx, inner)
        }
        ty::Adt(adt, generics) => {
            let id = adt.did();
            match (
                tcx.crate_name(id.krate).as_str(),
                tcx.item_name(id).as_str(),
            ) {
                ("core" | "std", "SocketAddr" | "SocketAddrV4" | "SocketAddrV6") => true,
                ("alloc" | "std", "Vec") | ("core" | "std", "Option") => {
                    holds_only_addresses(tcx, generics.type_at(0))
                }
                _ => false,
            }
        }
        ty::Tuple(fields) => match &fields[..] {
            [host, port] => is_ip(tcx, *host) && matches!(port.kind(), ty::Uint(ty::UintTy::U16)),
            _ => false,
        },
        _ => false,
    }
}

fn is_ip(tcx: TyCtxt<'_>, ty: Ty<'_>) -> bool {
    let ty::Adt(adt, _) = *ty.kind() else {
        return false;
    };
    let id = adt.did();
    matches!(tcx.crate_name(id.krate).as_str(), "core" | "std")
        && matches!(
            tcx.item_name(id).as_str(),
            "IpAddr" | "Ipv4Addr" | "Ipv6Addr"
        )
}

// A literal that is already an address: "127.0.0.1:0", "[::1]:80", or
// ("127.0.0.1", 0). A literal that only names a host is not.
fn literal_address<'tcx>(cx: &LateContext<'tcx>, argument: &'tcx Expr<'tcx>) -> bool {
    match peel_borrows(argument).kind {
        ExprKind::Tup([host, _]) => {
            literal_str(cx, host).is_some_and(|host| host.parse::<IpAddr>().is_ok())
        }
        _ => literal_str(cx, argument).is_some_and(|address| address.parse::<SocketAddr>().is_ok()),
    }
}

// The text of a string literal, `concat!` among them, or of a local const
// that is one.
fn literal_str<'tcx>(cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) -> Option<String> {
    let expr = peel_borrows(expr);
    match expr.kind {
        ExprKind::Lit(literal) => match literal.node {
            LitKind::Str(text, _) => Some(text.as_str().to_owned()),
            _ => None,
        },
        ExprKind::Path(ref path) => match cx.qpath_res(path, expr.hir_id) {
            Res::Def(DefKind::Const { .. }, id) => id
                .as_local()
                .and_then(|id| cx.tcx.hir_maybe_body_owned_by(id))
                .and_then(|body| literal_str(cx, body.value)),
            _ => None,
        },
        _ => None,
    }
}

fn peel_borrows<'tcx>(mut expr: &'tcx Expr<'tcx>) -> &'tcx Expr<'tcx> {
    while let ExprKind::AddrOf(_, _, inner) = expr.kind {
        expr = inner;
    }
    expr
}

#[test]
fn ui() {
    dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
}
